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

package io.snappydata.stream

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamTest, StreamingQuery, StreamingQueryException}


class MultipleSinkCommitTestSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  test("Commit Sequence in Multiple Sinks") {
    def startQuery(ds: Dataset[Int], queryName: String): StreamingQuery = {
      ds.writeStream
          .queryName(queryName)
          .format("memory")
          .start()
    }

    val input = MemoryStream[Int]
    var curOffSet = input.getOffset
    // scalastyle:off println
    println(curOffSet)
    val q1 = startQuery(input.toDS, "stream_test_1")
    val q2 = startQuery(input.toDS.map { i =>
      i
    }, "stream_test_2")
    val q3 = startQuery(input.toDS.map { i =>
      i
    }, "stream_test_3")
    try {
      input.addData(Seq(1, 2))
      q1.processAllAvailable()
      q2.processAllAvailable()
      q3.processAllAvailable()
      curOffSet = input.getOffset
      println(curOffSet)
      input.reset()
      curOffSet = input.getOffset
      println(curOffSet)
      input.addData(Seq(1, 2))
      q1.processAllAvailable()
      q2.processAllAvailable()
      q3.processAllAvailable()
      curOffSet = input.getOffset
      println(curOffSet)
      // scalastyle:on println
    } finally {
      q1.stop()
      q2.stop()
      q3.stop()
    }
  }

}
