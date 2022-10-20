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
         |  "reservoirVersion": 1,
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
        |  "reservoirVersion": 1,
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
        |  "reservoirVersion": 1,
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

  test("DeltaSharingSourceOffset - unmatched reservoir id") {
    val json =
      s"""
         |{
         |  "reservoirId": "${UUID.randomUUID().toString}",
         |  "sourceVersion": 1,
         |  "reservoirVersion": 1,
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
    DeltaSharingSourceOffset.validateOffsets(
      previousOffset = DeltaSharingSourceOffset(
        sourceVersion = 1,
        reservoirId = "foo",
        reservoirVersion = 4,
        index = 10,
        isStartingVersion = false),
      currentOffset = DeltaSharingSourceOffset(
        sourceVersion = 1,
        reservoirId = "foo",
        reservoirVersion = 4,
        index = 10,
        isStartingVersion = false)
    )
    DeltaSharingSourceOffset.validateOffsets(
      previousOffset = DeltaSharingSourceOffset(
        sourceVersion = 1,
        reservoirId = "foo",
        reservoirVersion = 4,
        index = 10,
        isStartingVersion = false),
      currentOffset = DeltaSharingSourceOffset(
        sourceVersion = 1,
        reservoirId = "foo",
        reservoirVersion = 5,
        index = 1,
        isStartingVersion = false)
    )

    assert(intercept[IllegalStateException] {
      DeltaSharingSourceOffset.validateOffsets(
        previousOffset = DeltaSharingSourceOffset(
          sourceVersion = 1,
          reservoirId = "foo",
          reservoirVersion = 4,
          index = 10,
          isStartingVersion = false),
        currentOffset = DeltaSharingSourceOffset(
          sourceVersion = 1,
          reservoirId = "foo",
          reservoirVersion = 4,
          index = 10,
          isStartingVersion = true)
      )
    }.getMessage.contains("Found invalid offsets: 'isStartingVersion' fliped incorrectly."))
    assert(intercept[IllegalStateException] {
      DeltaSharingSourceOffset.validateOffsets(
        previousOffset = DeltaSharingSourceOffset(
          sourceVersion = 1,
          reservoirId = "foo",
          reservoirVersion = 4,
          index = 10,
          isStartingVersion = false),
        currentOffset = DeltaSharingSourceOffset(
          sourceVersion = 1,
          reservoirId = "foo",
          reservoirVersion = 1,
          index = 10,
          isStartingVersion = false)
      )
    }.getMessage.contains("Found invalid offsets: 'reservoirVersion' moved back."))
    assert(intercept[IllegalStateException] {
      DeltaSharingSourceOffset.validateOffsets(
        previousOffset = DeltaSharingSourceOffset(
          sourceVersion = 1,
          reservoirId = "foo",
          reservoirVersion = 4,
          index = 10,
          isStartingVersion = false),
        currentOffset = DeltaSharingSourceOffset(
          sourceVersion = 1,
          reservoirId = "foo",
          reservoirVersion = 4,
          index = 9,
          isStartingVersion = false)
      )
    }.getMessage.contains("Found invalid offsets. 'index' moved back."))
  }
}
