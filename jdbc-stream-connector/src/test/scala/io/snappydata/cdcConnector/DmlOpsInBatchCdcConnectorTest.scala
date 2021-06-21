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

package io.snappydata.cdcConnector

import java.io._
import java.sql.{Connection, DriverManager, ResultSet}
import java.util
import java.util.{ArrayList, Properties, TimeZone}

import scala.sys.process._

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.SnappyFunSuite
import io.snappydata.app.DummySpec
import io.snappydata.test.dunit.AvailablePortHelper
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{IOFileFilter, TrueFileFilter, WildcardFileFilter}

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.streaming.jdbc.{JDBCSource, SnappyStoreSink, SnappyStreamSink}
import org.apache.spark.sql.{Dataset, Row, SnappySession}

/**
  * Steps for setting up environment for running this suite:
  * - It uses the SQL server connection with SQL Server instance from Azure cloud
  * (see `getSqlServerConnection()` method). So need to spin up the SQL Server instance
  * - Provide sql server connection properties in jdbc-stream-connector/src/main/
  * resources/confFiles/cdc_source_connection.properties file
  * - you may need to install `snappy-core_2.11` and snappy-jdbc-connector_2.11 jar
  * as part of local maven in case there are changes on snappydata repo which are
  * not published as part of the GA release
  * To publish the jars as part of local maven run `./gradlew install` under snappydata directory
  * - Check for snappydata core and jdbc-connector repos versions in pom.xml file under test/cdc
  * - It uses streaming jobs defined in test/cdc of `snappy-jdbc-connector` repo
  * - run `mvn clean package` under test/cdc to generate application jar
  * - run this suite:
  * `./gradlew :snappy-jdbc-connector_2.11:scalaTest -PsingleSuite=**DmlOpsInBatchCdcConnectorTest`
  */
class DmlOpsInBatchCdcConnectorTest extends SnappyFunSuite {

  private var testDir = ""
  private var appJar = ""
  var homeDir = ""

  val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort
  val props: Properties = new Properties
  var driver = ""
  var url = ""
  var username = ""
  var password = ""
  var snappyHome = ""
  var retries = 7

  override def beforeAll(): Unit = {
    TimeZone.setDefault(TimeZone.getDefault)
    snappyHome = System.getProperty("SNAPPY_HOME")
    if (snappyHome == null) {
      throw new Exception("SNAPPY_HOME should be set as a system property")
    }
    homeDir = System.getProperty("user.home")
    testDir = s"$snappyHome/../../../snappy-connectors/jdbc-stream-connector/src/test/resources"
    appJar = s"$snappyHome/../../../snappy-connectors/" +
        s"jdbc-stream-connector/src/test/cdc/target/cdc-test-0.0.1.jar"
    // appJar = s"$snappyHome/../../../snappy-poc/cdc/target/cdc-test-0.0.1.jar"
    props.load(new FileInputStream(s"$testDir/confFiles/cdc_source_connection.properties"))
    driver = props.getProperty("driver")
    url = props.getProperty("url")
    username = props.getProperty("user")
    password = props.getProperty("password")
    logInfo("Snappy home : " + snappyHome)
    logInfo("Home Dir : " + homeDir)
    logInfo("Test Config Dir : " + testDir)
    logInfo("App Jar location : " + appJar)

    setDMLMaxChunkSize(50L)
    before()

  }

  override def afterAll(): Unit = {
    after()
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  protected def getUserAppJarLocation(jarName: String, jarPath: String): String = {
    var userAppJarPath: String = null
    if (new File(jarName).exists) jarName
    else {
      val baseDir: File = new File(jarPath)
      try {
        val filter: IOFileFilter = new WildcardFileFilter(jarName)
        val files: util.List[File] = FileUtils.listFiles(baseDir, filter,
          TrueFileFilter.INSTANCE).asInstanceOf[util.List[File]]
        logInfo("Jar file found: " + util.Arrays.asList(files))
        import scala.collection.JavaConverters._
        for (file1: File <- files.asScala) {
          if (!file1.getAbsolutePath.contains("/work/") ||
              !file1.getAbsolutePath.contains("/scala-2.10/")) {
            userAppJarPath = file1.getAbsolutePath
          }

        }
      } catch {
        case e: Exception =>
          logInfo("Unable to find " + jarName
              + " jar at " + jarPath + " location.")
      }
      userAppJarPath
    }
  }

  def truncateSqlTable(): Unit = {

    // scalastyle:off classforname Class.forName

    Class.forName(driver)
    val conn = DriverManager.getConnection(
      url, username, password)
    conn.createStatement().execute("delete from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where (adj_id between 95000010061 and 950000100110)")
    conn.createStatement().execute("delete from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where (adj_id between 99999999999991 and 999999999999926)")
    Thread.sleep(50000)
    conn.createStatement().execute("truncate table testdatabase.cdc.dbo_ADJUSTMENT1_CT")

    val sqlDF = conn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        .executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
            "where (adj_id between 95000010061 and 950000100110)")

    if (sqlDF.next) logInfo("FAILURE : The result set should have been empty")
    else logInfo("SUCCESS : The result set is empty as expected")

    logInfo("Deleted rows from key 95000010061 to 950000100110" +
        " and 99999999999991 and 999999999999926")
    logInfo("Truncated cdc table")
  }

  def before(): Unit = {
    truncateSqlTable()

    logInfo((snappyHome + "/sbin/snappy-start-all.sh").!!)

    var filePath = s"$testDir/testCases/createTable.sql"
    var dataLocation = s"$snappyHome/../../../snappy-connectors/" +
        s"jdbc-stream-connector/build-artifacts/scala-2.11/libs"
    val connectorJar = getUserAppJarLocation("snappydata-jdbc-stream-connector_2.11-*.jar",
      dataLocation)

    val command = "/bin/snappy run -file=" + filePath +
        " -param:dataLocation=" + connectorJar + " -param:homeDirLocation=" + snappyHome +
        " -client-port=1527" + " -client-bind-address=localhost"

    logInfo((snappyHome + command).!!)

    val conn = getANetConnection(1527)
    var i = 95000010001L
    while ( i < 95000010099L) {

      val qStr = "INSERT INTO ADJUSTMENT1 VALUES (912, " + i + " , 9929," +
        " 102, 506161, 775324, 431703, 28440200.7189,'iT'," +
        " '2016-05-09 12:46:31.0', 520989, 966723, 592283," +
        " 682854, 165363, '2016-07-02 12:46:31.0', '2016-06-15 12:46:31.0','889','89');"
      logInfo("Query = " + qStr)

      conn.createStatement().execute(qStr)
      i +=1
    }

    val jobCommand = "/bin/snappy-job.sh submit " +
        "--app-name JavaCdcStreamingAppSnappyJob " +
        "--class io.snappydata.app.JavaCdcStreamingAppSnappyJob " +
        s"--conf configFile=$testDir/confFiles/" +
        "source_destination_tables.properties " +
        s"--conf configFile1=$testDir/confFiles/" +
        "cdc_source_connection.properties " +
        s"--app-jar $appJar " +
        "--lead localhost:8090"

    logInfo((snappyHome + jobCommand).!!)
  }

  def after(): Unit = {
    logInfo((snappyHome + "/sbin/snappy-stop-all.sh").!!)
  }

  def getSqlServerConnection: Connection = {
    Class.forName(driver)
    val conn = DriverManager.getConnection(
      url, username, password)
    conn
  }

  def getQuery(fileName: String): ArrayList[String] = {
    val queryList = new ArrayList[String]
    try {
      var s: String = new String()
      var sb: StringBuffer = new StringBuffer()

      val fr: FileReader = new FileReader(new File(fileName))
      var br: BufferedReader = new BufferedReader(fr)
      s = br.readLine()

      while (s != null) {
        sb.append(s)
        s = br.readLine()
      }
      br.close()
      var splitData: Array[String] = sb.toString().split(";")
      for (i <- 0 to splitData.length - 1) {
        {
          if (!(splitData(i) == null) || !(splitData(i).length == 0)) {
            val qry = splitData(i)
            logInfo("The query is " + qry)
            queryList.add(qry)
          }
        }
      }
    }

    catch {
      case e: FileNotFoundException => {
      }
      case io: IOException => {
      }
    }
    queryList
  }

  def executeQueries(queryArray: ArrayList[String], conn: Connection): Unit = {

    val statement = conn.createStatement()
    for (i <- 0 until queryArray.size()) {
      var query = queryArray.get(i)
      statement.executeUpdate(query)
    }
  }

  def getANetConnection(netPort: Int): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    DriverManager.getConnection("jdbc:snappydata:thrift://localhost[1527]")
  }

  def waitTillTheBatchIsPickedForProcessing(markerRecordId: BigInt, retries: Int): Unit = {
    logInfo("Retriessss " + retries)
    if (retries == 0) {
      throw new RuntimeException("Batch not received")
    }
    val pQuery = "select * from ADJUSTMENT1 where adj_id = " + markerRecordId + ";"
    val snappyconn = getANetConnection(1527)
    try {
      var rs = snappyconn.createStatement(
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(pQuery)
      if (!rs.next()) {
        Thread.sleep(15000)
        waitTillTheBatchIsPickedForProcessing(markerRecordId, retries - 1)
      }
    } finally {
      snappyconn.close()
    }
  }

  def collectData(queryPath: String, snappyQuery: String,
                  sqlQuery: String, markerRecordId: BigInt): Unit = {

    logInfo("Execute queries and print results")
    val qArr = getQuery(queryPath)
    val conn = getSqlServerConnection
    val snappyconn = getANetConnection(1527)
    executeQueries(qArr, conn)
    waitTillTheBatchIsPickedForProcessing(markerRecordId, retries)
    val snappyDF = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        .executeQuery(snappyQuery)
    val sqlResultSet = conn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        .executeQuery(sqlQuery)
    performValidation(sqlResultSet, snappyDF)
    conn.close()
    snappyconn.close()

  }

  def performValidation(sqlResultSet: ResultSet, snappyResultSet: ResultSet): Unit = {

    val resultMetaData = sqlResultSet.getMetaData
    val columnCnt = resultMetaData.getColumnCount
    val snappyMetadata = snappyResultSet.getMetaData
    val snappyColCnt = snappyMetadata.getColumnCount
    var sqlRowsCnt = 0
    while (sqlResultSet.next()) {
      sqlRowsCnt = sqlRowsCnt + 1
    }
    var snappyRowsCnt = 0
    while (snappyResultSet.next()) {
      snappyRowsCnt = snappyRowsCnt + 1
    }
    sqlResultSet.beforeFirst()
    snappyResultSet.beforeFirst()
    logInfo("Row cnt of snappy = " + snappyRowsCnt + " and sql table is = "
        + sqlRowsCnt)
    logInfo("Column cnt of snappy = " + snappyColCnt + " and sql table is = "
        + columnCnt)
    if (snappyRowsCnt.equals(sqlRowsCnt)) {
      while (sqlResultSet.next()) {
        snappyResultSet.next()
        for (i <- 1 to columnCnt) {
          if (sqlResultSet.getObject(i).equals(snappyResultSet.getObject(i))) {
            logInfo("match " + sqlResultSet.getObject(i) + " "
                + snappyResultSet.getObject(i))
          }
          else {
            throw new Exception("not match " + sqlResultSet.getObject(i)
                + " " + snappyResultSet.getObject(i))
          }
        }
      }
    }
    else {
      throw new Exception("Row cnt of snappy = " + snappyRowsCnt + " and sql table is = "
          + sqlRowsCnt + " not matching")
    }
  }

  test("Test for insert,update,delete and insert happening " +
      "sequentially on same key in one batch") {

    val queryString = s"$testDir/testCases/testCase1.sql"
    val snQuery = "select * from ADJUSTMENT1 where adj_id = 95000010061;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] where adj_id = 95000010061;"

    collectData(queryString, snQuery, sqlQuery, 99999999999991L)
  }

  test("Test for insert,delete and insert happening sequentially on same key in one batch") {

    val queryString = s"$testDir/testCases/testCase2.sql"
    val snQuery = "select * from ADJUSTMENT1 where adj_id = 95000010062;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] where adj_id = 95000010062;"

    collectData(queryString, snQuery, sqlQuery, 99999999999992L)
  }

  test("Test for insert,delete and insert happening sequentially on two keys in one batch") {

    val queryString = s"$testDir/testCases/testCase3.sql"
    val snQuery = "select * from ADJUSTMENT1 " +
        "where (adj_id between 95000010063 and 95000010064) order by adj_id;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where (adj_id between 95000010063 and 95000010064) order by adj_id;"

    collectData(queryString, snQuery, sqlQuery, 99999999999993L)
  }

  test("Test for insert,delete,insert and update " +
      "happening sequentially on same key in one batch") {

    val queryString = s"$testDir/testCases/testCase4.sql"
    val snQuery = "select * from ADJUSTMENT1 where adj_id = 95000010065;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where adj_id = 95000010065;"

    collectData(queryString, snQuery, sqlQuery, 99999999999994L)
  }

  test("Test for multiple inserts,deletes and " +
      "inserts happening sequentially on keys in one batch") {

    val queryString = s"$testDir/testCases/testCase5.sql"
    val snQuery = "select * from ADJUSTMENT1 " +
        "where (adj_id between 95000010066 and 95000010067) order by adj_id;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where (adj_id between 95000010066 and 95000010067) order by adj_id;"

    collectData(queryString, snQuery, sqlQuery, 99999999999995L)
  }

  test("Test for multiple inserts,deletes,inserts " +
      "and updates happening sequentially on keys in one batch") {

    val queryString = s"$testDir/testCases/testCase6.sql"
    var snQuery = "select * from ADJUSTMENT1 " +
        "where (adj_id between 95000010068 and 95000010069) order by adj_id;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where (adj_id between 95000010068 and 95000010069) order by adj_id;"

    collectData(queryString, snQuery, sqlQuery, 99999999999996L)
  }

  test("Test for insert,delete,insert and update " +
      "happening sequentially on same key having diff lsn in one batch") {

    val queryString = s"$testDir/testCases/testCase7.sql"
    var snQuery = "select * from ADJUSTMENT1 " +
        "where adj_id = 95000010070;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where adj_id = 95000010070;"

    collectData(queryString, snQuery, sqlQuery, 99999999999997L)
  }

  test("Test for insert,delete and insert happening " +
      "sequentially on same key having diff lsn in one batch") {

    val queryString = s"$testDir/testCases/testCase8.sql"
    var snQuery = "select * from ADJUSTMENT1 " +
        "where adj_id = 95000010071;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where adj_id = 95000010071;"

    collectData(queryString, snQuery, sqlQuery, 99999999999998L)
  }

  test("Test for multiple inserts,deletes and inserts " +
      "happening sequentially on keys having diff lsn in one batch") {

    val queryString = s"$testDir/testCases/testCase9.sql"
    var snQuery = "select * from ADJUSTMENT1 " +
        "where (adj_id between 95000010072 and 95000010073) order by adj_id;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where (adj_id between 95000010072 and 95000010073) order by adj_id;"

    collectData(queryString, snQuery, sqlQuery, 99999999999999L)
  }

  test("Test for insert and update happening sequentially on same key in one batch") {

    val queryString = s"$testDir/testCases/testCase10.sql"
    val snQuery = "select * from ADJUSTMENT1 " +
        "where adj_id = 95000010074;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where adj_id = 95000010074;"

    collectData(queryString, snQuery, sqlQuery, 999999999999910L)
  }

  test("Test for insert and update happening sequentially" +
      " on same key having diff lsn in one batch") {

    val queryString = s"$testDir/testCases/testCase11.sql"
    var snQuery = "select * from ADJUSTMENT1 " +
        "where adj_id = 95000010075;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where adj_id = 95000010075;"

    collectData(queryString, snQuery, sqlQuery, 999999999999911L)
  }

  test("Test for insert update,delete,insert and " +
      "update happening sequentially on same key in one batch") {

    val queryString = s"$testDir/testCases/testCase12.sql"
    var snQuery = "select * from ADJUSTMENT1 " +
        "where adj_id = 95000010076;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where adj_id = 95000010076;"

    collectData(queryString, snQuery, sqlQuery, 999999999999912L)
  }

  test("Test for multiple inserts,deletes,inserts and " +
      "updates happening sequentially on diff keys in one batch") {

    val queryString = s"$testDir/testCases/testCase13.sql"
    var snQuery = "select * from ADJUSTMENT1 " +
        "where (adj_id between 95000010077 and 95000010080) order by adj_id;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where (adj_id between 95000010077 and 95000010080) order by adj_id;"

    collectData(queryString, snQuery, sqlQuery, 999999999999913L)
  }

  test("Test for multiple inserts,updates,deletes,inserts " +
      "and updates happening sequentially in one batch") {

    val queryString = s"$testDir/testCases/testCase14.sql"
    var snQuery = "select * from ADJUSTMENT1 " +
        "where (adj_id between 95000010081 and 95000010084) order by adj_id;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where (adj_id between 95000010081 and 95000010084) order by adj_id;"

    collectData(queryString, snQuery, sqlQuery, 999999999999914L)
  }

  test("Test for multiple inserts and updates happening sequentially in one batch") {

    val queryString = s"$testDir/testCases/testCase15.sql"
    var snQuery = "select * from ADJUSTMENT1 " +
        "where (adj_id between 95000010085 and 95000010088) order by adj_id;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where (adj_id between 95000010085 and 95000010088) order by adj_id;"

    collectData(queryString, snQuery, sqlQuery, 999999999999915L)
  }

  test("Test for multiple inserts happening sequentially in one batch") {

    val queryString = s"$testDir/testCases/testCase16.sql"
    var snQuery = "select * from ADJUSTMENT1 " +
        "where (adj_id between 95000010089 and 95000010092) order by adj_id;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where (adj_id between 95000010089 and 95000010092) order by adj_id;"

    collectData(queryString, snQuery, sqlQuery, 999999999999916L)
  }

  test("Test for single insert in one batch") {

    val queryString = s"$testDir/testCases/testCase17.sql"
    var snQuery = "select * from ADJUSTMENT1 " +
        "where adj_id = 95000010093;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where adj_id = 95000010093;"

    collectData(queryString, snQuery, sqlQuery, 999999999999917L)
  }

  test("Test for single update in one batch") {

    val queryString = s"$testDir/testCases/testCase18.sql"
    var snQuery = "select * from ADJUSTMENT1 " +
        "where adj_id = 95000010093;"
    val sqlQuery = "select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
        "where adj_id = 95000010093;"

    collectData(queryString, snQuery, sqlQuery, 999999999999918L)
  }

  test("Test for single delete in one batch") {

    val queryString = s"$testDir/testCases/testCase19.sql"
    val qArr = getQuery(queryString)
    val conn = getSqlServerConnection
    val snappyconn = getANetConnection(1527)
    executeQueries(qArr, conn)
    waitTillTheBatchIsPickedForProcessing(999999999999919L, retries)
    val snappyDF = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        .executeQuery("select * from ADJUSTMENT1 " +
            "where adj_id = 95000010093;")
    if (snappyDF.next) logInfo("FAILURE : The result set should have been empty")
    else logInfo("SUCCESS : The result set is empty as expected for 95000010093")
    conn.close()
    snappyconn.close()

  }

  test("Test for multiple inserts and deletes happening sequentially in one batch") {

    val queryString = s"$testDir/testCases/testCase20.sql"
    val qArr = getQuery(queryString)
    val conn = getSqlServerConnection
    val snappyconn = getANetConnection(1527)
    executeQueries(qArr, conn)
    waitTillTheBatchIsPickedForProcessing(999999999999920L, retries)
    val snappyDF = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        .executeQuery("select * from ADJUSTMENT1 " +
            "where (adj_id between 95000010094 and 95000010097) order by adj_id;")
    if (snappyDF.next) logInfo("FAILURE : The result set should have been empty")
    else logInfo("SUCCESS : The result set is empty as expected")
    conn.close()
    snappyconn.close()

  }

  test("Test for multiple inserts,updates and deletes happening sequentially in one batch") {

    val queryString = s"$testDir/testCases/testCase21.sql"
    val qArr = getQuery(queryString)
    val conn = getSqlServerConnection
    val snappyconn = getANetConnection(1527)
    executeQueries(qArr, conn)
    waitTillTheBatchIsPickedForProcessing(999999999999921L, retries)
    val snappyDF = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        .executeQuery("select * from ADJUSTMENT1 " +
            "where (adj_id between 95000010098 and 950000100100) order by adj_id;")
    if (snappyDF.next) logInfo("FAILURE : The result set should have been empty")
    else logInfo("SUCCESS : The result set is empty as expected")
    conn.close()
    snappyconn.close()

  }

  test("Test for insert,update and delete happening sequentially on two diff key in one batch") {

    val queryString = s"$testDir/testCases/testCase22.sql"
    val qArr = getQuery(queryString)
    val conn = getSqlServerConnection
    val snappyconn = getANetConnection(1527)
    executeQueries(qArr, conn)
    waitTillTheBatchIsPickedForProcessing(999999999999922L, retries)
    val snappyDF = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        .executeQuery("select * from ADJUSTMENT1 " +
            "where (adj_id between 950000100101 and 950000100102) order by adj_id;")
    if (snappyDF.next) logInfo("FAILURE : The result set should have been empty")
    else logInfo("SUCCESS : The result set is empty as expected")
    conn.close()
    snappyconn.close()

  }

  test("Test for insert,update and delete happening sequentially on diff keys in one batch") {

    val queryString = s"$testDir/testCases/testCase23.sql"
    val qArr = getQuery(queryString)
    val conn = getSqlServerConnection
    val snappyconn = getANetConnection(1527)
    executeQueries(qArr, conn)
    waitTillTheBatchIsPickedForProcessing(999999999999923L, retries)
    val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT1 " +
        "where adj_id = 950000100103;")
    if (snappyDF.next) logInfo("FAILURE : The result set should have been empty")
    else logInfo("SUCCESS : The result set is empty as expected for 950000100103")
    val query = "select descr from ADJUSTMENT1 where adj_id = 950000100104 and descr = 'iT';"
    var rs1 = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(query)
    while (!rs1.next()) {
      rs1 = snappyconn.createStatement(
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
          executeQuery(query)
    }
    val snappyDF1 = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        .executeQuery("select * from ADJUSTMENT1 " +
            "where adj_id = 950000100104;")
    val sqlResultSet1 = conn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
        executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
            "where adj_id = 950000100104;")
    performValidation(sqlResultSet1, snappyDF1)
    conn.close()
    snappyconn.close()

  }

  test("Test for dml ops happening sequentially in one batch") {

    val queryString = s"$testDir/testCases/testCase24.sql"
    val qArr = getQuery(queryString)
    val conn = getSqlServerConnection
    val snappyconn = getANetConnection(1527)
    executeQueries(qArr, conn)
    waitTillTheBatchIsPickedForProcessing(999999999999924L, retries)
    val snappyDF = snappyconn.createStatement().executeQuery("select * from ADJUSTMENT1 " +
        "where adj_id = 950000100105;")
    if (snappyDF.next) logInfo("FAILURE : The result set should have been empty")
    else logInfo("SUCCESS : The result set is empty as expected for 950000100105")
    val query = "select descr from ADJUSTMENT1 where adj_id = 950000100106 and descr = 'iTTTz';"
    var rs1 = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(query)
    while (!rs1.next()) {
      rs1 = snappyconn.createStatement(
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
          executeQuery(query)
    }
    val snappyDF1 = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        .executeQuery("select * from ADJUSTMENT1 " +
            "where adj_id = 950000100106;")
    val sqlResultSet = conn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
        executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
            "where adj_id = 950000100106;")
    performValidation(sqlResultSet, snappyDF1)
    conn.close()
    snappyconn.close()

  }

  test("Test for dml ops happening sequentially on diff keys in one batch") {

    val queryString = s"$testDir/testCases/testCase25.sql"
    val qArr = getQuery(queryString)
    val conn = getSqlServerConnection
    val snappyconn = getANetConnection(1527)
    executeQueries(qArr, conn)
    waitTillTheBatchIsPickedForProcessing(999999999999925L, retries)
    val snappyDF = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        .executeQuery("select * from ADJUSTMENT1 " +
            "where adj_id = 950000100107;")
    if (snappyDF.next) logInfo("FAILURE : The result set should have been empty")
    else logInfo("SUCCESS : The result set is empty as expected for 950000100107")
    val query = "select descr from ADJUSTMENT1 where adj_id = 950000100108 and descr = 'iTTTzy';"
    var rs1 = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(query)
    while (!rs1.next()) {
      rs1 = snappyconn.createStatement(
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
          executeQuery(query)
    }
    val snappyDF1 = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        .executeQuery("select * from ADJUSTMENT1 " +
            "where adj_id = 950000100108;")
    val sqlResultSet = conn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
        executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
            "where adj_id = 950000100108;")
    performValidation(sqlResultSet, snappyDF1)
    conn.close()
    snappyconn.close()

  }

  test("Test for insert and multiple updates happening sequentially on same key in one batch") {

    val queryString = s"$testDir/testCases/testCase26.sql"
    val qArr = getQuery(queryString)
    val conn = getSqlServerConnection
    val snappyconn = getANetConnection(1527)
    executeQueries(qArr, conn)
    waitTillTheBatchIsPickedForProcessing(999999999999926L, retries)
    val query = "select descr from ADJUSTMENT1 where adj_id = 950000100109" +
        " and POST_DT = '2016-05-10' and DESCR = 'iTTyt'" +
        " and SRC_SYS_REF_ID = '90' and SRC_SYS_REC_ID = '890';"
    var rs = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(query)
    while (!rs.next()) {
      rs = snappyconn.createStatement(
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
          executeQuery(query)
    }
    val snappyDF = snappyconn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        .executeQuery("select * from ADJUSTMENT1 " +
            "where adj_id = 950000100109;")
    val sqlResultSet = conn.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).
        executeQuery("select * from [testdatabase].[dbo].[ADJUSTMENT1] " +
            "where adj_id = 950000100109;")
    performValidation(sqlResultSet, snappyDF)
    conn.close()
    snappyconn.close()

  }

  test("JDBCSource toString returns params with masked password") {
    val params = new CaseInsensitiveMap(Map("user" -> username,
      "password" -> password, "sourceTableName" -> "table1",
      "url" -> url, "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver"))
    val jdbcSource = JDBCSource(snc.snappySession.sqlContext, params,
      new DummySpec)
    val expected = s"JDBCSource[${params.updated("password", "****")}]"
    assertResult(expected)(jdbcSource.toString)
  }

  test("Sink description") {
    val params = Map("key1" -> "value1", "key2" -> "value2")
    val sink = SnappyStoreSink(null, params, new DummySink)
    val expectedDescription = "SnappyStoreSink[key1 -> value1, key2 -> value2, " +
        "sinkCallback -> io.snappydata.cdcConnector.DmlOpsInBatchCdcConnectorTest$DummySink]"
    assertResult(expectedDescription)(sink.toString)
  }

  class DummySink extends SnappyStreamSink {
    override def process(snappySession: SnappySession, sinkProps: Map[String, String]
        , batchId: Long, df: Dataset[Row]): Unit = {}
  }
}
