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


import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.jdbc.StreamConf._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SnappyContext, SnappySession}
import org.apache.spark.util.Utils

trait SnappyStreamSink {
  def process(snappySession: SnappySession, sinkProps: Map[String, String],
      batchId: Long, df: Dataset[Row]): Unit
}

class SnappyStoreSinkProvider extends StreamSinkProvider with DataSourceRegister {

  @Override
  def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val cc = Utils.classForName(parameters("sink")).newInstance()
    SnappyStoreSink(sqlContext.asInstanceOf[SnappyContext].snappySession, parameters,
      cc.asInstanceOf[SnappyStreamSink])
  }

  @Override
  def shortName(): String = SNAPPY_SINK

}

case class SnappyStoreSink(snappySession: SnappySession,
    parameters: Map[String, String], sink: SnappyStreamSink) extends Sink {

  @Override
  def addBatch(batchId: Long, data: Dataset[Row]): Unit = {
    sink.process(snappySession, parameters, batchId, convert(data))
  }

  /**
    * This conversion is necessary as Sink
    * documentation disallows an operation on incoming dataframe.
    * Otherwise it will break incremental planning of streaming dataframes.
    * See http://apache-spark-developers-list.1001551.n3.nabble.com/
    * Structured-Streaming-Sink-in-2-0-collect-foreach-restrictions-added-in-
    * SPARK-16020-td18118.html
    * for a detailed discussion.
    */
  def convert(ds: DataFrame): DataFrame = {
    snappySession.internalCreateDataFrame(
      ds.queryExecution.toRdd,
      StructType(ds.schema.fields))
  }

  override def toString: String = {
    val params = parameters + ("sinkCallback" -> sink.getClass.getName)
    s"SnappyStoreSink[${params.mkString(", ")}]"
  }
}
