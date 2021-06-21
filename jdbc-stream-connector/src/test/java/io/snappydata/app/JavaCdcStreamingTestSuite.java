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
package io.snappydata.app;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SnappySession;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.jdbc.StreamConf;
import org.apache.spark.util.Utils;
import scala.collection.Seq;

import static scala.collection.JavaConversions.seqAsJavaList;

public class JavaCdcStreamingTestSuite {

  /**
   * A map of source and destination table. The source table should have been defined in
   * the source database and destination table in SnappyData
   */
  private final java.util.Map<String, String> sourceDestTables = new LinkedHashMap<>();

  private java.util.Map<String, String> sourceOptions = new HashMap<>();

  private SnappySession snappySpark;

  public static void main(String[] args) throws Exception {
    JavaCdcStreamingTestSuite _this = new JavaCdcStreamingTestSuite();
    _this.startJob();
  }

  private SnappySession createSnappySession(String table) throws ClassNotFoundException, IOException {
    String checkPointDir = Utils.createTempDir(".", "stream-spark-" + table).getCanonicalPath();
    snappySpark = new SnappySession(SparkSession.builder().
        appName("TestIngestion").
        master("local[4]").
        config("spark.sql.streaming.checkpointLocation", checkPointDir).
        config("snappydata.connection", "localhost:1527").
        config("spark.sql.autoBroadcastJoinThreshold", "-1").
        config("snappydata.cache.putIntoInnerJoinResult", "true").
        config("spark.task.maxFailures", "0").
        config("spark.sql.defaultSizeInBytes", "10000").
        getOrCreate().sparkContext());
    sourceOptions = fillSourceOptions(snappySpark.sparkContext().conf());
    return snappySpark;
  }

  private void startJob() throws Exception {
    configureTables();
    ArrayList<StreamingQuery> activeQueries = new ArrayList<>(sourceDestTables.size());

    for (String sourceTable : sourceDestTables.keySet()) {
      SnappySession newSession = createSnappySession(sourceTable);
      DataStreamReader reader = newSession.readStream()
          .format(StreamConf.JDBC_STREAM())
          .option(StreamConf.SPEC(), "io.snappydata.app.DummySpec")
          .option(StreamConf.SOURCE_TABLE_NAME(), sourceTable)
          .option(StreamConf.MAX_EVENTS(), "50000")
          .options(sourceOptions);


      Dataset<Row> ds = reader.load();
      StreamingQuery q = getStreamWriter(sourceDestTables.get(sourceTable), ds);
      activeQueries.add(q);
    }

    for (StreamingQuery q : activeQueries) {
      q.awaitTermination();
    }
  }

  private StreamingQuery getStreamWriter(String tableName,
      Dataset<Row> reader) throws IOException {

    Seq<Column> keyColumns = snappySpark.sessionCatalog().getKeyColumns(tableName);

    String keyColsCSV = seqAsJavaList(keyColumns).stream()
        .map(Column::name).collect(Collectors.joining(","));

    System.out.println("Key Columns are : " + keyColsCSV);
    return reader.writeStream()
        .trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
        .format(StreamConf.SNAPPY_SINK())
        .option("sink", ProcessEvents.class.getName())
        .option("tableName", tableName)
        .option("keyColumns", keyColsCSV)
        .option("handleconflict", "true")
        .start();
  }

  private static java.util.Map<String, String> fillSourceOptions(SparkConf conf) {
    java.util.Map<String, String> options = new HashMap<>();

    if (!conf.contains("driver")) {
      options.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
    } else {
      options.put("driver", conf.get("driver"));
    }
    if (!conf.contains("url")) {
      options.put("url", "jdbc:sqlserver://sqlent.westus.cloudapp.azure.com:1433;DatabaseName=testdatabase");
    } else {
      options.put("url", conf.get("url"));
    }
    if (!conf.contains("user")) {
      options.put("user", "sqldb");
    } else {
      options.put("user", conf.get("user"));
    }
    if (!conf.contains("password")) {
      options.put("password", "snappydata#msft1");
    } else {
      options.put("password", conf.get("password"));
    }

    if (!conf.contains("databaseName")) {
      options.put("databaseName", "testdatabase");
    } else {
      options.put("databaseName", conf.get("databaseName"));
    }

    /*if (!conf.contains("poolImpl")) {
      options.put("poolImpl", "tomcat");
    } else {
      options.put("poolImpl", conf.get("poolImpl"));
    }

    if (!conf.contains("poolProperties")) {
      options.put("poolProperties", "validationQuery=select 1,minIdle=5");
    } else {
      options.put("poolProperties", conf.get("poolProperties"));
    }*/

    if (!conf.contains("poolImpl")) {
      options.put("poolImpl", "hikari");
    } else {
      options.put("poolImpl", conf.get("poolImpl"));
    }

    if (!conf.contains("poolProperties")) {
      options.put("poolProperties", "connectionTestQuery=select 1,minimumIdle=5");
    } else {
      options.put("poolProperties", conf.get("poolProperties"));
    }

    return options;
  }

  private void configureTables() {
    sourceDestTables.put("testdatabase.cdc.dbo_adjustment_CT", "adjustment");
    // sourceDestTables.put("testdatabase.cdc.dbo_person_event_CT ", "person_event");
    // sourceDestTables.put("testdatabase.cdc.dbo_CODE_VALUE_CT", "CODE_VALUE");
  }

}
