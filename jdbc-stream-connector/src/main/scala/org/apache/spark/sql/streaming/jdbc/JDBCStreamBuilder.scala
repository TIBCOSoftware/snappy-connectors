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

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{AnalysisException, Dataset, Row, SnappySession}
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import scala.collection.JavaConverters._

import org.apache.spark.api.java.function.{Function => JFunction}

object StreamConf {

  val SPEC = "spec"
  val MAX_EVENTS = "maxEvents"
  val SOURCE_TABLE_NAME = "sourceTableName"
  val WRITE_TABLE_NAME = "writeTableName"
  val TRIGGER_TIME = "triggerTime"
  val JDBC_STREAM = "jdbcStream"
  val SNAPPY_SINK = "snappystore"
  val DEFAULT_TRIGGER_TIME = "60"
  val DEFAULT_MAX_EVENTS = "50000"
}

class JDBCStreamBuilder(val snappySession: SnappySession) {

  import StreamConf._

  private[this] val config = new scala.collection.mutable.HashMap[String, String]

  var customFunction: Option[(Dataset[Row]) => Dataset[Row]] = None
  var customJFunction: Option[JFunction[Dataset[Row], Dataset[Row]]] = None

  def spec(profileClassName: String): JDBCStreamBuilder = {
    config.put(SPEC, profileClassName)
    this
  }

  def maxEvents(maximumEvents: Int): JDBCStreamBuilder = {
    config.put(MAX_EVENTS, maximumEvents.toString)
    this
  }

  def sourceTable(tableName: String): JDBCStreamBuilder = {
    config.put(SOURCE_TABLE_NAME, tableName)
    this
  }

  def writeTable(tableName: String): JDBCStreamBuilder = {
    config.put(WRITE_TABLE_NAME, tableName)
    this
  }

  def connectionParams(options: Map[String, String]): JDBCStreamBuilder = {
    options.foreach { case (k, v) => config.put(k, v) }
    this
  }

  def connectionParams(options: java.util.Map[String, String]): JDBCStreamBuilder = {
    options.asScala.foreach { case (k, v) => config.put(k, v) }
    this
  }

  def customTransformations(f: (Dataset[Row]) => Dataset[Row]): JDBCStreamBuilder = {
    customFunction = Some(f)
    this
  }

  def customTransformations(f: JFunction[Dataset[Row], Dataset[Row]]): JDBCStreamBuilder = {
    customJFunction = Some(f)
    this
  }

  /**
    * Polling interval from source in seconds. Default is 60 seconds
    */
  def pollInterval(s: Int): JDBCStreamBuilder = {
    config.put(TRIGGER_TIME, s.toString)
    this
  }

  def start(): StreamingQuery = {

    val sourceTab = config.get(SOURCE_TABLE_NAME)
        .getOrElse(throw new AnalysisException("Source Table name not specified"))
    val destTab = config.get(WRITE_TABLE_NAME)
        .getOrElse(throw new AnalysisException("Write Table name not specified"))

    val profile = config.get(SPEC)
        .getOrElse(throw new AnalysisException("CDC Source Spec not specified"))

    val reader = snappySession.readStream
        .format(JDBC_STREAM).options(config).option(SOURCE_TABLE_NAME, sourceTab)
        .option(SPEC, profile)

    val ds = reader.load

    val transFormedDs = if (customFunction.isDefined) {
      customFunction.get(ds)
    } else if (customJFunction.isDefined) {
      customJFunction.get.call(ds)
    } else {
      ds
    }
    val pollInterval = config.getOrElse(TRIGGER_TIME, DEFAULT_TRIGGER_TIME).toInt
    transFormedDs.writeStream.trigger(ProcessingTime.create(pollInterval, TimeUnit.SECONDS))
        .format(SNAPPY_SINK)
        .option(WRITE_TABLE_NAME, destTab)
        .option(SPEC, profile)
        .start
  }
}
