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

class DeltaSharingSourceSuite extends QueryTest
  with SharedSparkSession with DeltaSharingIntegrationTest {

  import testImplicits._

  // VERSION 0: CREATE TABLE
  // VERSION 1: INSERT 3 rows, 3 add files
  // VERSION 2: REMOVE 1 row, 1 remove file
  // VERSION 3: UPDATE 1 row, 1 remove file and 1 add file
  lazy val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_cdf_enabled"

  lazy val deltaLog = RemoteDeltaLog(tablePath)

  val streamingTimeout = 60.seconds

  def getSource(parameters: Map[String, String]): DeltaSharingSource = {
    val options = new DeltaSharingOptions(parameters)
    DeltaSharingSource(SparkSession.active, deltaLog, options)
  }

  def withStreamReaderAtVersion(startingVersion: String = "0"): DataStreamReader = {
    spark.readStream.format("deltaSharing").option("path", tablePath)
      .option("startingVersion", startingVersion)
      .option("ignoreDeletes", "true")
      .option("ignoreChanges", "true")
  }

  // TODO: add test on a shared table without schema
  // TODO: support dir so we can test checkpoint, restart the stream, etc.
  // TODO: test TriggerAvailableNow

  /**
   * Test defaultReadLimit
   */
  integrationTest("DeltaSharingSource - defaultLimit") {
    val source = getSource(Map.empty[String, String])

    val defaultLimit = source.getDefaultReadLimit
    assert(defaultLimit.isInstanceOf[ReadMaxFiles])
    assert(defaultLimit.asInstanceOf[ReadMaxFiles].maxFiles ==
      DeltaSharingOptions.MAX_FILES_PER_TRIGGER_OPTION_DEFAULT)
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
    assert(offset.tableVersion == 6)
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
   * Test schema exception
   */
  integrationTest("disallow user specified schema") {
    var message = intercept[UnsupportedOperationException] {
      val query = spark.readStream.format("deltaSharing").option("path", tablePath)
        .schema(StructType(Array(StructField("a", TimestampType), StructField("b", StringType))))
        .load()
    }.getMessage
    assert(message.contains("Delta sharing does not support specifying the schema at read time"))
  }

  /**
   * Test basic streaming functionality
   */
  integrationTest("basic - success") {
    val query = withStreamReaderAtVersion()
      .load().writeStream.format("console").start()

    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 4)
      }
    } finally {
      query.stop()
    }
  }

  integrationTest("no startingVersion - success") {
    // cdf_table_cdf_enabled snapshot at version 5 is queried, with 2 files and 2 rows of data
    val query = spark.readStream.format("deltaSharing").option("path", tablePath)
      .option("ignoreDeletes", "true")
      .option("ignoreChanges", "true")
      .load().writeStream.format("console").start()

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
  }

  /**
   * Test maxFilesPerTrigger and maxBytesPerTrigger
   */
  integrationTest("maxFilesPerTrigger - success with different values") {
    // Map from maxFilesPerTrigger to a list, the size of the list is the number of progresses of
    // the stream query, and each element in the list is the numInputRows for each progress.
    Map(1 -> Seq(1, 1, 1, 1), 2 -> Seq(2, 2), 3 -> Seq(3, 1), 4 -> Seq(4), 5 -> Seq(4)).foreach{
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

  testQuietly("maxFilesPerTrigger - invalid parameter") {
    Seq("0", "-1", "string").foreach { invalidMaxFilesPerTrigger =>
      val message = intercept[IllegalArgumentException] {
        val query = withStreamReaderAtVersion()
          .option("maxFilesPerTrigger", invalidMaxFilesPerTrigger)
          .load().writeStream.format("console").start()
      }.getMessage
      for (msg <- Seq("Invalid", DeltaSharingOptions.MAX_FILES_PER_TRIGGER_OPTION, "positive")) {
        assert(message.contains(msg))
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
        assert(p.numInputRows === 4)
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

  /**
   * Test ignoreChanges/ignoreDeletes
   */
  integrationTest("ignoreDeletes/ignoreChanges - are needed to process deletes/updates") {
    // There are deletes at version 2 of cdf_table_cdf_enabled
    var query = spark.readStream.format("deltaSharing").option("path", tablePath)
      .option("startingVersion", "0")
      .load().writeStream.format("console").start()
    var message = intercept[StreamingQueryException] {
      query.awaitTermination() // block until query is terminated, with stop() or with error
    }.getMessage
    assert(message.contains("Detected deleted data from streaming source at version 2"))

    // There are updates at version 3 of cdf_table_cdf_enabled
    query = spark.readStream.format("deltaSharing").option("path", tablePath)
      .option("startingVersion", "0")
      .option("ignoreDeletes", "true")
      .load().writeStream.format("console").start()
    message = intercept[StreamingQueryException] {
      query.awaitTermination() // block until query is terminated, with stop() or with error
    }.getMessage
    assert(message.contains("Detected a data update in the source table at version 3"))
  }

  /**
   * Test readChangeFeed/readchangeData
   */
  integrationTest("readChangeFeed/readchangeData - not supported yet") {
    var message = intercept[UnsupportedOperationException] {
      val query = spark.readStream.format("deltaSharing").option("path", tablePath)
        .option("startingVersion", "0")
        .option("readChangeFeed", "true")
        .load().writeStream.format("console").start()
    }.getMessage
    assert(message.contains("Delta Sharing Streaming CDF is not supported yet"))

    message = intercept[UnsupportedOperationException] {
      val query = spark.readStream.format("deltaSharing").option("path", tablePath)
        .option("startingVersion", "0")
        .option("readChangeData", "true")
        .load().writeStream.format("console").start()
    }.getMessage
    assert(message.contains("Delta Sharing Streaming CDF is not supported yet"))
  }

  /**
   * Test versionAsOf/timestampAsOf
   */
  integrationTest("versionAsOf/timestampAsOf - not supported") {
    var message = intercept[UnsupportedOperationException] {
      val query = spark.readStream.format("deltaSharing").option("path", tablePath)
        .option("versionAsOf", "1")
        .load().writeStream.format("console").start()
    }.getMessage
    assert(message.contains("Cannot time travel streams"))

    message = intercept[UnsupportedOperationException] {
      val query = spark.readStream.format("deltaSharing").option("path", tablePath)
        .option("timestampAsOf", "2022-10-01 00:00:00.0")
        .load().writeStream.format("console").start()
    }.getMessage
    assert(message.contains("Cannot time travel streams"))
  }

  /**
   * Test startingVersion/startingTimestamp
   */
  integrationTest("startingVersion/startingTimestamp - exceptions") {
    Seq("-1", "string").foreach { invalidStartingVersion =>
      val message = intercept[IllegalArgumentException] {
        val query = spark.readStream.format("deltaSharing").option("path", tablePath)
          .option("startingVersion", invalidStartingVersion)
          .load().writeStream.format("console").start()
      }.getMessage
      for (msg <- Seq("Invalid", DeltaSharingOptions.STARTING_VERSION_OPTION, "greater")) {
        assert(message.contains(msg))
      }
    }


    var message = intercept[StreamingQueryException] {
      val query = spark.readStream.format("deltaSharing").option("path", tablePath)
        .option("startingTimestamp", "")
        .load().writeStream.format("console").start()
      query.awaitTermination(streamingTimeout.toMillis)
    }.getMessage
    assert(message.contains("Invalid startingTimestamp:"))
  }

  integrationTest("startingTimestamp - succeeds") {
    // 2022-01-01 00:00:00.0 a timestamp before version 0, which will be converted to version 0.
    var query = spark.readStream.format("deltaSharing").option("path", tablePath)
      .option("startingTimestamp", "2022-01-01 00:00:00.0")
      .option("ignoreDeletes", "true")
      .option("ignoreChanges", "true")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 4)
      }
    } finally {
      query.stop()
    }
  }

  integrationTest("startingVersion - succeeds") {
    var query = withStreamReaderAtVersion("1")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 4)
      }
    } finally {
      query.stop()
    }

    query = withStreamReaderAtVersion("2")
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

    query = withStreamReaderAtVersion("3")
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

    // there are 3 versions in total
    query = withStreamReaderAtVersion("4")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 0)
    } finally {
      query.stop()
    }
  }
}
