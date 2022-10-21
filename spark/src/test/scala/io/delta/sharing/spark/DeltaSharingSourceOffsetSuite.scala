/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.sharing.spark

import java.util.UUID

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.streaming.ReadMaxFiles
import org.apache.spark.sql.execution.streaming.SerializedOffset
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQueryException, Trigger}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.scalatest.time.SpanSugar._

class DeltaSharingSourceOffsetSuite extends QueryTest
  with SharedSparkSession with DeltaSharingIntegrationTest {

  import testImplicits._

  test("DeltaSharingSourceOffset sourceVersion - unknown value") {
    // Set unknown sourceVersion as the max allowed version plus 1.
    var unknownVersion = 2

    val json =
      s"""
         |{
         |  "sourceVersion": $unknownVersion,
         |  "tableVersion": 1,
         |  "index": 1,
         |  "isStartingVersion": true
         |}
      """.stripMargin
    val e = intercept[IllegalStateException] {
      DeltaSharingSourceOffset(UUID.randomUUID().toString, SerializedOffset(json))
    }
    assert(e.getMessage.contains("Please upgrade to a new release"))
  }

  test("DeltaSharingSourceOffset sourceVersion - invalid value") {
    val json =
      """
        |{
        |  "sourceVersion": "foo",
        |  "tableVersion": 1,
        |  "index": 1,
        |  "isStartingVersion": true
        |}
      """.stripMargin
    val e = intercept[IllegalStateException] {
      DeltaSharingSourceOffset(UUID.randomUUID().toString, SerializedOffset(json))
    }
    for (msg <- Seq("foo", "invalid")) {
      assert(e.getMessage.contains(msg))
    }
  }

  test("DeltaSharingSourceOffset sourceVersion - missing ") {
    val json =
      """
        |{
        |  "tableVersion": 1,
        |  "index": 1,
        |  "isStartingVersion": true
        |}
      """.stripMargin
    val e = intercept[IllegalStateException] {
      DeltaSharingSourceOffset(UUID.randomUUID().toString, SerializedOffset(json))
    }
    for (msg <- Seq("Cannot find", "sourceVersion")) {
      assert(e.getMessage.contains(msg))
    }
  }

  test("DeltaSharingSourceOffset - unmatched table id") {
    val json =
      s"""
         |{
         |  "tableId": "${UUID.randomUUID().toString}",
         |  "sourceVersion": 1,
         |  "tableVersion": 1,
         |  "index": 1,
         |  "isStartingVersion": true
         |}
      """.stripMargin
    val e = intercept[IllegalStateException] {
      DeltaSharingSourceOffset(UUID.randomUUID().toString, SerializedOffset(json))
    }
    for (msg <- Seq("delete", "checkpoint", "restart")) {
      assert(e.getMessage.contains(msg))
    }
  }

  test("DeltaSharingSourceOffset - validateOffsets") {
    def testValidateOffset(
      previousTableVersion: Long,
      previousIndex: Long,
      previousIsStarting: Boolean,
      currentTableVersion: Long,
      currentIndex: Long,
      currentIsStarting: Boolean,
      errorMessage: Option[String]
    ): Unit = {
      val previousOffset = DeltaSharingSourceOffset(
        sourceVersion = 1,
        tableId = "foo",
        tableVersion = previousTableVersion,
        index = previousIndex,
        isStartingVersion = previousIsStarting)
      val currentOffset = DeltaSharingSourceOffset(
        sourceVersion = 1,
        tableId = "foo",
        tableVersion = currentTableVersion,
        index = currentIndex,
        isStartingVersion = currentIsStarting)
      if (errorMessage.isDefined) {
        assert(intercept[IllegalStateException] {
          DeltaSharingSourceOffset.validateOffsets(previousOffset, currentOffset)
        }.getMessage.contains(errorMessage.get))
      } else {
        DeltaSharingSourceOffset.validateOffsets(previousOffset, currentOffset)
      }
    }

    // No errors on forward moving offset
    testValidateOffset(4, 10, false, 4, 10, false, None)
    testValidateOffset(4, 10, false, 4, 11, false, None)
    testValidateOffset(4, 10, false, 5, 1, false, None)
    testValidateOffset(4, 10, true, 4, 10, true, None)
    testValidateOffset(4, 10, true, 4, 11, true, None)
    testValidateOffset(4, 10, true, 5, 1, true, None)

    // errors on backward moving offset
    testValidateOffset(4, 10, false, 4, 9, false, Some("Found invalid offsets. Previous:"))
    testValidateOffset(4, 10, false, 3, 11, false, Some("Found invalid offsets. Previous:"))
    testValidateOffset(4, 10, true, 4, 9, true, Some("Found invalid offsets. Previous:"))
    testValidateOffset(4, 10, true, 3, 11, true, Some("Found invalid offsets. Previous:"))

    // isStartingVersion flipping from true to false: ok
    testValidateOffset(4, 10, true, 4, 10, false, None)
    // isStartingVersion flipping from false to true: error
    testValidateOffset(4, 10, false, 4, 10, true, Some(
      "Found invalid offsets: 'isStartingVersion' fliped incorrectly."))
  }
}
