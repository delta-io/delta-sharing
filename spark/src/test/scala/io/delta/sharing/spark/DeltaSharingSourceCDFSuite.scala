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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.streaming.ReadMaxFiles
import org.apache.spark.sql.execution.streaming.SerializedOffset
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQueryException, Trigger}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.scalatest.time.SpanSugar._

class DeltaSharingSourceCDFSuite extends QueryTest
  with SharedSparkSession with DeltaSharingIntegrationTest {

  import testImplicits._

  // VERSION 0: CREATE TABLE
  // VERSION 1: INSERT 3 rows, 3 add files
  // VERSION 2: REMOVE 1 row, 1 remove file
  // VERSION 3: UPDATE 1 row, 1 remove file and 1 add file
  lazy val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_cdf_enabled"

  // allowed to query starting from version 1
  // VERSION 1: INSERT 3 rows, 3 add files
  // VERSION 2: UPDATE 1 row, 1 cdf file
  // VERSION 2: REMOVE 1 row, 1 remove file
  lazy val cdfTablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_with_partition"

  lazy val deltaLog = RemoteDeltaLog(cdfTablePath)

  val streamingTimeout = 5.seconds

  def getSource(parameters: Map[String, String]): DeltaSharingSource = {
    val options = new DeltaSharingOptions(parameters ++ Map("readChangeFeed" -> "true"))
    DeltaSharingSource(SparkSession.active, deltaLog, options)
  }

  def withStreamReaderAtVersion(
      path: String = cdfTablePath,
      startingVersion: String = "1"): DataStreamReader = {
    spark.readStream.format("deltaSharing").option("path", path)
      .option("startingVersion", startingVersion)
      .option("ignoreDeletes", "true")
      .option("ignoreChanges", "true")
      .option("readChangeFeed", "true")
  }

  /**
   * Test latestOffset
   */
  integrationTest("DeltaSharingSource.latestOffset - startingVersion 0") {
    val source = getSource(Map(
      "ignoreChanges" -> "true",
      "ignoreDeletes" -> "true",
      "startingVersion" -> "0"
    ))
    val latestOffset = source.latestOffset(null, source.getDefaultReadLimit)

    assert(latestOffset.isInstanceOf[DeltaSharingSourceOffset])
    val offset = latestOffset.asInstanceOf[DeltaSharingSourceOffset]
    assert(offset.sourceVersion == 1)
    assert(offset.tableId == deltaLog.snapshot(Some(0)).metadata.id)
    assert(offset.tableVersion == 4)
    assert(offset.index == -1)
    assert(!offset.isStartingVersion)
  }

  integrationTest("DeltaSharingSource.latestOffset - startingVersion latest") {
    val source = getSource(Map(
      "ignoreChanges" -> "true",
      "ignoreDeletes" -> "true",
      "startingVersion" -> "latest"
    ))
    val latestOffset = source.latestOffset(null, source.getDefaultReadLimit)
    assert(latestOffset == null)
  }

  integrationTest("DeltaSharingSource.latestOffset - no startingVersion") {
    val source = getSource(Map("ignoreChanges" -> "true", "ignoreDeletes" -> "true"))
    val latestOffset = source.latestOffset(null, source.getDefaultReadLimit)

    assert(latestOffset.isInstanceOf[DeltaSharingSourceOffset])
    val offset = latestOffset.asInstanceOf[DeltaSharingSourceOffset]
    assert(offset.sourceVersion == 1)
    assert(offset.tableId == deltaLog.snapshot().metadata.id)
    assert(offset.tableVersion == 4)
    assert(offset.index == -1)
    assert(!offset.isStartingVersion)
  }

  /**
   * Test getBatch
   */
  integrationTest("DeltaSharingSource.getBatch - exception") {
    // getBatch cannot be called without writestream
    val source = getSource(Map(
      "ignoreChanges" -> "true",
      "ignoreDeletes" -> "true",
      "startingVersion" -> "0"))
    val latestOffset = source.latestOffset(null, source.getDefaultReadLimit)
    intercept[AnalysisException] {
      // Error message would be:
      //    Queries with streaming sources must be executed with writeStream.start()
      source.getBatch(None, latestOffset.asInstanceOf[DeltaSharingSourceOffset]).show()
    }
  }

  /**
   * Test basic cdf streaming functionality
   */
  integrationTest("CDF Stream basic - success") {
    var query = withStreamReaderAtVersion()
      .load().writeStream.format("console").start()

    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 6)
      }
    } finally {
      query.stop()
    }

    // Test readchangeData
    query = spark.readStream.format("deltaSharing")
      .option("readchangeData", "true")
      .option("startingVersion", "1")
      .load(cdfTablePath).writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 6)
      }
    } finally {
      query.stop()
    }
  }

  integrationTest("CDF Stream select columns - success") {
    // select all columns in a different order
    var query = withStreamReaderAtVersion()
      .load()
      .select("birthday", "_commit_version", "name", "_commit_timestamp", "age", "_change_type")
      .writeStream.format("console").start()

    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 6)
      }
    } finally {
      query.stop()
    }

    // select a few columns
    query = withStreamReaderAtVersion()
      .load()
      .select("birthday", "_commit_version", "age", "_change_type")
      .writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 6)
      }
    } finally {
      query.stop()
    }
  }

  integrationTest("CDF Stream - exceptions") {
    // For tablePath(cdf_table_cdf_enabled), cdf is disabled at version 4, so there will exception
    // returned from the server.
    Console.println(s"-----[linzhou]--------exception-1")
    var query = withStreamReaderAtVersion(path = tablePath)
      .load().writeStream.format("console").start()
    var message = intercept[StreamingQueryException] {
      // block until query is terminated, with stop() or with error
      query.awaitTermination(streamingTimeout.toMillis)
    }.getMessage
    assert(message.contains("Error getting change data for range"))

    Console.println(s"-----[linzhou]--------exception-2")
    query = withStreamReaderAtVersion(startingVersion = "0")
      .load().writeStream.format("console").start()
    message = intercept[StreamingQueryException] {
      // block until query is terminated, with stop() or with error
      query.awaitTermination(streamingTimeout.toMillis)
    }.getMessage
    assert(message.contains("You can only query table changes since version 1."))

    // startingTimestamp > latest version
    Console.println(s"-----[linzhou]--------exception-3")
    message = intercept[StreamingQueryException] {
      val query = spark.readStream.format("deltaSharing")
        .option("readchangeFeed", "true")
        .option("startingTimestamp", "9999-01-01 00:00:00.0")
        .load(cdfTablePath).writeStream.format("console").start()
      query.awaitTermination(streamingTimeout.toMillis)
    }.getMessage
    assert(message.contains("The provided timestamp (9999-01-01 00:00:00.0) is after"))

    // startingTimestamp corresponds to version 0, < allowed starting version 1.
    message = intercept[StreamingQueryException] {
      val query = spark.readStream.format("deltaSharing")
        .option("readchangeFeed", "true")
        .option("startingTimestamp", "2022-01-01 00:00:00")
        .load(cdfTablePath).writeStream.format("console").start()
      query.awaitTermination(streamingTimeout.toMillis)
    }.getMessage
    assert(message.contains("The provided timestamp(2022-01-01 00:00:00) corresponds to 0"))
    // startingVersion > latest version
  }

  integrationTest("CDF Stream - different startingVersion") {
    // Starting from version 2: 1 CDF File
    var query = withStreamReaderAtVersion(startingVersion = "2")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 3)
      }
    } finally {
      query.stop()
    }

    // Starting from version 3: 1 Remove File
    query = withStreamReaderAtVersion(startingVersion = "3")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 1)
      }
    } finally {
      query.stop()
    }


    // TODO: timestamp for queryTable with version
    // no startingVersion - should result fetch the entire snapshot
    query = spark.readStream.format("deltaSharing")
      .option("readchangeFeed", "true")
      .load(cdfTablePath).writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 2)
      }
    } finally {
      query.stop()
    }

    // latest startingVersion - should fetch no data
    query = spark.readStream.format("deltaSharing")
      .option("readchangeFeed", "true")
      .option("startingVersion", "latest")
      .load(cdfTablePath).writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 0)
    } finally {
      query.stop()
    }
  }

  integrationTest("CDF Stream - startingTimestamp works") {
//    val query = spark.readStream.format("deltaSharing")
//      .option("readchangeFeed", "true")
//      .option("startingTimestamp", "2022-01-01 00:00:00")
//      .load(cdfTablePath).writeStream.format("console").start()
//    try {
//      query.processAllAvailable()
//      val progress = query.recentProgress.filter(_.numInputRows != 0)
//      assert(progress.length === 0)
//    } finally {
//      query.stop()
//    }
  }


  /**
   * Test maxFilesPerTrigger and maxBytesPerTrigger
   */
  integrationTest("maxFilesPerTrigger - success with different values") {
    // Map from maxFilesPerTrigger to a list, the size of the list is the number of progresses of
    // the stream query, and each element in the list is the numInputRows for each progress.
    Map(1 -> Seq(1, 1, 1, 2, 1), 2 -> Seq(2, 2, 1),
        3 -> Seq(3, 2), 4 -> Seq(4, 1), 5 -> Seq(5)).foreach{
      case (k, v) =>
        val query = withStreamReaderAtVersion()
          .option("maxFilesPerTrigger", s"$k")
          .load().writeStream.format("console").start()

        try {
          query.processAllAvailable()
          val progress = query.recentProgress.filter(_.numInputRows != 0)
          assert(progress.length === v.size)
          progress.zipWithIndex.map { case (p, index) =>
            assert(p.numInputRows === v(index))
          }
        } finally {
          query.stop()
        }
    }
  }

  test("maxFilesPerTrigger - ignored when using Trigger.Once") {
    val query = withStreamReaderAtVersion()
      .option("maxFilesPerTrigger", "1")
      .load().writeStream.format("console")
      .trigger(Trigger.Once)
      .start()

    try {
      assert(query.awaitTermination(streamingTimeout.toMillis))
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1) // only one trigger was run
      progress.foreach { p =>
        assert(p.numInputRows === 6)
      }
    } finally {
      query.stop()
    }
  }

  integrationTest("maxBytesPerTrigger - at least one file") {
    // Map from maxBytesPerTrigger to a list, the size of the list is the number of progresses of
    // the stream query, and each element in the list is the numInputRows for each progress.
    Map(1 -> Seq(1, 1, 1, 1), 1000 -> Seq(1, 1, 1, 1), 2000 -> Seq(2, 2),
      3000 -> Seq(3, 1), 4000 -> Seq(4), 5000 -> Seq(4)).foreach {
      case (k, v) =>
        val query = withStreamReaderAtVersion()
          .option("maxBytesPerTrigger", s"${k}b")
          .load().writeStream.format("console").start()

        try {
          query.processAllAvailable()
          val progress = query.recentProgress.filter(_.numInputRows != 0)
          assert(progress.length === v.size)
          progress.zipWithIndex.map { case (p, index) =>
            assert(p.numInputRows === v(index))
          }
        } finally {
          query.stop()
        }
    }
  }

  testQuietly("maxBytesPerTrigger - invalid parameter") {
    Seq("0", "-1", "string").foreach { invalidMaxFilesPerTrigger =>
      val message = intercept[IllegalArgumentException] {
        val query = withStreamReaderAtVersion()
          .option("maxBytesPerTrigger", invalidMaxFilesPerTrigger)
          .load().writeStream.format("console").start()
      }.getMessage
      for (msg <- Seq("Invalid", DeltaSharingOptions.MAX_BYTES_PER_TRIGGER_OPTION, "size")) {
        assert(message.contains(msg))
      }
    }
  }

  test("maxBytesPerTrigger - ignored when using Trigger.Once") {
    val query = withStreamReaderAtVersion()
      .option("maxBytesPerTrigger", "1b")
      .load().writeStream.format("console")
      .trigger(Trigger.Once)
      .start()

    try {
      assert(query.awaitTermination(streamingTimeout.toMillis))
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1) // only one trigger was run
      progress.foreach { p =>
        assert(p.numInputRows === 4)
      }
    } finally {
      query.stop()
    }
  }

  test("maxBytesPerTrigger - max bytes and max files together") {
    // should process one file at a time
    val q = withStreamReaderAtVersion()
      .option(DeltaSharingOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
      .option(DeltaSharingOptions.MAX_BYTES_PER_TRIGGER_OPTION, "100gb")
      .load().writeStream.format("console").start()
    try {
      q.processAllAvailable()
      val progress = q.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 4)
      progress.foreach { p =>
        assert(p.numInputRows === 1)
      }
    } finally {
      q.stop()
    }

    val q2 = withStreamReaderAtVersion()
      .option(DeltaSharingOptions.MAX_FILES_PER_TRIGGER_OPTION, "2")
      .option(DeltaSharingOptions.MAX_BYTES_PER_TRIGGER_OPTION, "1b")
      .load().writeStream.format("console").start()
    try {
      q2.processAllAvailable()
      val progress = q2.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 4)
      progress.foreach { p =>
        assert(p.numInputRows === 1)
      }
    } finally {
      q2.stop()
    }
  }
}
