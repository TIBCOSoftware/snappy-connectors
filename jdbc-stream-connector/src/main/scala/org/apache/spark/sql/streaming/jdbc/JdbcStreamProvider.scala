/*
 * Copyright (c) 2017-2021 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.streaming.jdbc

import java.sql.{DriverManager, ResultSet}
import java.util.Properties

import org.apache.spark.jdbc.ConnectionConf
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCPartition, JDBCRelationUtil}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter, StreamSourceProvider}
import org.apache.spark.sql.streaming.jdbc.StreamConf._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.jdbc.StreamConf._
import org.apache.spark.sql.types.{DateType, NumericType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SnappyContext, SnappySession}
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, Partition}

class JdbcStreamProvider extends StreamSourceProvider with DataSourceRegister {

  @Override
  def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    val spec = Utils.classForName(parameters(SPEC)).
        newInstance().asInstanceOf[SourceSpec]
    (parameters(SOURCE_TABLE_NAME), JDBCSource(sqlContext, parameters, spec).schema)
  }

  @Override
  def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val spec = Utils.classForName(parameters(SPEC)).
        newInstance().asInstanceOf[SourceSpec]
    JDBCSource(sqlContext, parameters, spec)
  }

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def shortName(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  override def shortName(): String = JDBC_STREAM
}

case class LSN(override val json: String) extends Offset

case class JDBCSource(@transient sqlContext: SQLContext,
    parameters: Map[String, String], val spec : SourceSpec) extends Source with Logging {
  private val paramsWithMaskedPW: Map[String, String] = if (parameters.contains("password")) {
    parameters.updated("password", "****")
  } else parameters

  private val dbtable = parameters(SOURCE_TABLE_NAME)

  private val offsetColumn = spec.offsetColumn

  private val offsetToStrFunc = spec.offsetToStrFunc.getOrElse("")

  private val strToOffsetFunc = spec.strToOffsetFunc.getOrElse("")

  private val projectedColumns = spec.projectedColumns

  private val maxEvents = getParam(MAX_EVENTS).getOrElse(DEFAULT_MAX_EVENTS).toInt

  private val snappySession = sqlContext.asInstanceOf[SnappyContext].snappySession

  private val APP_NAME = sqlContext.sparkContext.appName

  private val streamStateTable = s"SNAPPY_JDBC_STREAM_OFFSET_TABLE"

  private val partitioner = getParam("partitionBy")

  private val conf : ConnectionConf =
    JDBCRelationUtil.buildConf(sqlContext.sparkSession.asInstanceOf[SnappySession], parameters)

  type PartitionQueryType = (String, Boolean, Boolean) // (query, isPreOrdered, isPreRanged)

  private val partitionByQuery: Option[PartitionQueryType] = getParam("partitionByQuery") match {
    case Some(q) if q.startsWith("ordered:") => Some((q.substring("ordered:".length), true, false))
    case Some(q) if q.startsWith("ranged:") =>
      // if ranged, we should assume ordered.
      Some((q.substring("ranged:".length), true, true))
    case Some(q) => Some((q, false, false))
    case None => None
  }

  private val cachePartitioningValuesFrom = getParam("cachePartitioningValuesFrom")

  private val reader =
    parameters.foldLeft(sqlContext.sparkSession.read)({
      case (r, (k, v)) => r.option(k, v)
    })

  private val srcControlConn = {
    val connProps = parameters.foldLeft(new Properties()) {
      case (prop, (k, v)) if !k.toLowerCase.startsWith("snappydata.") =>
        prop.setProperty(k, v)
        prop
      case (p, _) => p
    }
    DriverManager.getConnection(parameters("url"), connProps)
  }

  createContextTableIfNotExist()

  private def lastPersistedOffset: Option[LSN] = {
    trySessionQuery("select Last_Offset from " +
        s"$streamStateTable where App_name = '$APP_NAME' and Table_Name = '$dbtable'") { ds =>
      val rows = ds.collect()
      if (rows.length > 0) {
        Some(LSN(rows(0)(0).asInstanceOf[String]))
      } else {
        None
      }
    }
  }

  private var lastOffset: Option[LSN] = lastPersistedOffset

  private lazy val cachedPartitions = cachePartitioningValuesFrom match {
    case Some(q) if partitioner.nonEmpty =>
      determinePartitions(
        s"( ${q.replaceAll("$partitionBy", partitioner.get)} ) getUniqueValuesForCaching",
        partitioner.get)
    case _ => Array.empty[String]
  }

  /**
   * Returns the maximum available offset for this source.
   */
  override def getOffset: Option[Offset] = {
    def fetch(rs: ResultSet) = {
      if (rs.next() && rs.getString(1) != null) {
        Some(LSN(rs.getString(1)))
      } else {
        None
      }
    }

    lastOffset = lastOffset match {
      case None =>
        trySourceQuery(
          s"select $offsetToStrFunc(min($offsetColumn)) newOffset from $dbtable")(fetch)
      case Some(offset) =>
        trySourceQuery(getNextOffsetQuery(offset))(fetch) match {
          case None => Some(offset)
          case o => o
        }
    }
    lastOffset
  }

  private def lsn(o: Offset) : String = o.asInstanceOf[LSN].json

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {

    def getPartitions(query: String) = partitioner match {
      case _ if cachedPartitions.nonEmpty => cachedPartitions
      // if partition by column name have a query, assume it will yeild
      // predefined ranges per row.
      case Some(col) if partitionByQuery.nonEmpty =>
        val parallelism = sqlContext.sparkContext.defaultParallelism
        val (rangeQuery, isPreOrdered, isAlreadyRanged) = partitionByQuery.map { case (q, o, r) =>
          (q.replace("$getBatch", query).replace("$parallelism", parallelism.toString),
              o, r)
        }.get
        determinePartitions(s"( $rangeQuery ) getRanges", col,
          ordered = isPreOrdered,
          ranged = isAlreadyRanged)
      case Some(col) =>
        val uniqueValQuery = query.replace("select * from",
          s"select distinct($col) parts from ").replace("getBatch", "getUniqueValuesFromBatch")
        determinePartitions(uniqueValQuery, col)
      case None => query.replace("getBatch", "")
        Array.empty[String]
    }
    logDebug("lastPersistedOffset =  " + lastPersistedOffset
        + " Current start offset = " + start)
    start match {
      case None => lastPersistedOffset match {
        case None =>
          execute(s"(select $projectedColumns from $dbtable" +
              s" where $offsetColumn <= $strToOffsetFunc('${lsn(end)}')) getBatch" +
              s" ORDER by $offsetColumn ASC", getPartitions)
        case Some(offset) =>
          alertUser(s"Resuming source [${parameters("url")}] [$dbtable] " +
              s"from persistent offset -> $offset ")
          execute(s"(select $projectedColumns from $dbtable" +
              s" where $offsetColumn > $strToOffsetFunc('${offset.json}') " +
              s" and $offsetColumn <= $strToOffsetFunc('${lsn(end)}')) getBatch" +
              s" ORDER by $offsetColumn ASC", getPartitions)
      }
      case Some(begin) =>
        execute(s"(select $projectedColumns from $dbtable" +
            s" where $offsetColumn > $strToOffsetFunc('${lsn(begin)}') " +
            s" and $offsetColumn <= $strToOffsetFunc('${lsn(end)}')) getBatch" +
            s" ORDER by $offsetColumn ASC", getPartitions)
    }
  }

  override def commit(end: Offset): Unit = try {
    logDebug("Commiting " + end.json)
    // update the state table at source.
    val rowsEffected = snappySession.update(streamStateTable,
      s"APP_NAME = '$APP_NAME' and TABLE_NAME = '$dbtable'", Row(end.json), "LAST_OFFSET")
    if (rowsEffected == 0) {
      val rowsInserted = snappySession.insert(streamStateTable, Row(APP_NAME, dbtable, end.json))
      assert(rowsInserted == 1)
    }
    logDebug(s"Committed successfully $end")
  } catch {
    case e: Exception =>
      logWarning(s"Error committing offset $end", e)
  }

  /** Returns the schema of the data from this source */
  override def schema: StructType = {
    JDBCRelationUtil.schema(dbtable, parameters).
        add(new StructField("STRLSN", StringType, nullable = false)).
        add(new StructField("LSNTOTIME", StringType, nullable = false))
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = if (srcControlConn != null) srcControlConn.close()


  private def determinePartitions(query: String,
      partCol: String,
      ordered: Boolean = false,
      ranged: Boolean = false): Array[String] = {
    val uniqueVals = execute(query)
    val partitionField = uniqueVals.schema.fields(0)
    val sorted = if (ordered) {
      logDebug(s"Assuming already ordered while determining partitions. query -> $query")
      // already ordered
      uniqueVals.collect()
    } else {
      // just one partition so sortWithinPartitions is enough
      uniqueVals.sortWithinPartitions(partitionField.name).collect()
    }

    if (ranged) {
      logDebug(s"Assuming already ranged while determining partitions. query -> $query")
      // expect both inclusive ranges in 1st and 2nd column
      sorted.map { row =>
        partitionField.dataType match {
          case _: NumericType =>
            s"$partCol >= ${row(0)} and $partCol <= ${row(1)}"
          case _@(StringType | DateType | TimestampType) =>
            s"$partCol >= '${row(0)}' and $partCol <= '${row(1)}'"
        }
      }
    } else {
      val parallelism = sqlContext.sparkContext.defaultParallelism
      val slide = sorted.length / parallelism
      if (slide > 0) {
        sorted.sliding(slide, slide).map { a =>
          partitionField.dataType match {
            case _: NumericType =>
              s"$partCol >= ${a(0)(0)} and $partCol < ${a(a.length - 1)(0)}"
            case _@(StringType | DateType | TimestampType) =>
              s"$partCol >= '${a(0)(0)}' and $partCol < '${a(a.length - 1)(0)}'"
          }
        }.toArray
      } else {
        Array.empty[String]
      }
    }
  }

  private def createContextTableIfNotExist() = {
    snappySession.sql(s"create table if not exists $streamStateTable(" +
      " App_Name varchar(200)," +
      " Table_Name varchar(200)," +
      " Last_Offset varchar(100), " +
      " PRIMARY KEY (App_Name, Table_Name)) using row options(DISKSTORE 'GFXD-DD-DISKSTORE')")
  }

  private def _execute(query: String,
      partitions: String => Array[String] = _ => Array.empty) = partitions(query) match {
    case parts if parts.nonEmpty =>
      logDebug(s"About to execute with ${parts.mkString(",")} -> $query")
      reader.jdbc(parameters("url"), query, parts, new Properties())
    case _ =>
      logDebug(s"About to execute to determine partitions -> $query")
      reader.jdbc(parameters("url"), query, new Properties())
  }

  private def trySourceQuery[T](query: String)(f: ResultSet => T) = {
    logDebug(s"Executing -> $query")
    val rs = srcControlConn.createStatement().executeQuery(query)
    try {
      f(rs)
    } finally {
      rs.close()
    }
  }

  private def execute(query: String,
      partitions: String => Array[String] = _ => Array.empty): Dataset[Row] = {
    val filters: Array[Filter] = Array.empty[Filter]
    val parts = partitions(query) match {
      case parts if parts.nonEmpty => parts.zipWithIndex.map { case (part, i) =>
        JDBCPartition(part, i): Partition
      }
      case _ => Array[Partition](JDBCPartition(null, 0))
    }
    val tableSchema = schema
    val requiredColumns = tableSchema.map(f => f.name).toArray
    logDebug(s"About to execute query -> $query")
    JDBCRelationUtil.jdbcDF(sqlContext.sparkSession.asInstanceOf[SnappySession],
      parameters,
      query,
      tableSchema,
      requiredColumns,
      parts,
      conf,
      filters)
  }


  private def trySessionQuery[T](query: String)(f: DataFrame => T) = {
    logDebug(s"Executing -> $query")
    val ds = snappySession.sql(query)
    f(ds)
  }


  private def getNextOffsetQuery(lastOffset: LSN): String = {
     substituteQueryVariables(spec.getNextOffset(dbtable, lsn(lastOffset), maxEvents))
  }

  private def alertUser(msg: => String): Unit = {
    logInfo(msg)
  }

  private def getParam(p: String): Option[String] = {
    def returnIfNotNull(v: String) = if (v != null && v.length > 0) Some(v) else None

    parameters.get(s"$dbtable.$p").fold(
      parameters.get(p).flatMap(returnIfNotNull))(returnIfNotNull)
  }

  private def substituteQueryVariables(q: String): String = q.replaceAll("\\$table", dbtable)
      .replaceAll("\\$offsetToStrFunc", offsetToStrFunc)
      .replaceAll("\\$strToOffsetFunc", strToOffsetFunc)

  override def toString: String = s"JDBCSource[$paramsWithMaskedPW]"
}
