/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

trait SourceSpec {

  /** A fixed offset column name in the source table. */
  def offsetColumn: String

  /** An optional function to convert offset
    * to string format, if offset is not in string format.
    */
  def offsetToStrFunc: Option[String]

  /** An optional function to convert string offset to native offset format. */
  def strToOffsetFunc: Option[String]

  /** Query to find the next offset */
  def getNextOffset(tableName : String, currentOffset : String, maxEvents : Int): String

  def projectedColumns: String
}

class SqlServerSpec extends SourceSpec {

  override def offsetColumn: String = "__$start_lsn"

  override def offsetToStrFunc: Option[String] = Some("master.dbo.fn_varbintohexstr")

  override def strToOffsetFunc: Option[String] = Some("master.dbo.fn_cdc_hexstrtobin")

  override def projectedColumns: String =
      s"""master.dbo.fn_varbintohexstr($offsetColumn) as STRLSN,
          sys.fn_cdc_map_lsn_to_time($offsetColumn) as LSNTOTIME,
          *"""

  /** make sure to convert the LSN to string and give a column alias for the
    * user defined query one is supplying. Alternatively, a procedure can be invoked
    * with $table, $currentOffset Snappy provided variables returning one string column
    * representing the next LSN string.
    */
  override def getNextOffset(tableName : String, currentOffset : String, maxEvents : Int): String =
    s"""select master.dbo.fn_varbintohexstr(max(${offsetColumn})) nextLSN
         from (select ${offsetColumn}, sum(count(1)) over
         (order by ${offsetColumn}) runningCount
        from $tableName where ${offsetColumn} > master.dbo.fn_cdc_hexstrtobin('$currentOffset')
          group by ${offsetColumn}) x where runningCount <= $maxEvents"""

}
