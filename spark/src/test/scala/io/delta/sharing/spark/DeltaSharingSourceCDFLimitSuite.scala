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

import org.apache.spark.sql.{QueryTest}
import org.apache.spark.sql.streaming.{DataStreamReader, Trigger}
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.time.SpanSugar._

class DeltaSharingSourceCDFLimitSuite extends QueryTest
  with SharedSparkSession with DeltaSharingIntegrationTest {

  val streamingTimeout = 30.seconds

  /**
   * Test maxFilesPerTrigger and maxBytesPerTrigger on CDF Streaming Query
   */

  // allowed to query starting from version 1
  // VERSION 1: INSERT 2 rows, 1 add file
  // VERSION 2: INSERT 3 rows, 1 add file
  // VERSION 3: UPDATE 4 rows, 4 cdf files, 8 cdf rows
  // VERSION 4: REMOVE 4 rows, 2 remove files
  lazy val cdfTablePath = testProfileFile.getCanonicalPath + "#share8.default.streaming_cdf_table"

  def withCDFStreamReaderAtVersion(
      path: String = cdfTablePath,
      startingVersion: String = "0"): DataStreamReader = {
    spark.readStream.format("deltaSharing").option("path", path)
      .option("startingVersion", startingVersion)
      .option("ignoreDeletes", "true")
      .option("ignoreChanges", "true")
      .option("readChangeFeed", "true")
  }

  integrationTest("maxFilesPerTrigger on CDF - success with different values") {
    // Map from maxFilesPerTrigger to a list, the size of the list is the number of progresses of
    // the stream query, and each element in the list is the numInputRows for each progress.
    //
    // Table versions:
    // VERSION 1: INSERT 2 rows, 1 add file
    // VERSION 2: INSERT 3 rows, 1 add file
    // VERSION 3: UPDATE 4 rows, 4 cdf files, 8 cdf rows,
    //   For CDC commits we either admit the entire commit or nothing at all.
    //   This is to avoid returning `update_preimage` and `update_postimage` in separate
    //   batches.
    // VERSION 4: REMOVE 4 rows, 2 remove files
    Map(1 -> Seq(2, 3, 8, 2, 2), 2 -> Seq(5, 8, 4), 3 -> Seq(13, 4),
      6 -> Seq(13, 4), 7 -> Seq(15, 2), 8 -> Seq(17), 9 -> Seq(17)).foreach{
      case (k, v) =>
        val query = withCDFStreamReaderAtVersion()
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

  integrationTest("maxFilesPerTrigger on CDF - ignored when using Trigger.Once") {
    val query = withCDFStreamReaderAtVersion()
      .option("maxFilesPerTrigger", "1")
      .load().writeStream.format("console")
      .trigger(Trigger.Once)
      .start()

    try {
      assert(query.awaitTermination(streamingTimeout.toMillis))
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1) // only one trigger was run
      progress.foreach { p =>
        assert(p.numInputRows === 17)
      }
    } finally {
      query.stop()
    }
  }

  integrationTest("maxBytesPerTrigger on CDF - success with different values") {
    // Map from maxBytesPerTrigger to a list, the size of the list is the number of progresses of
    // the stream query, and each element in the list is the numInputRows for each progress.
    //
    // Table versions:
    // VERSION 1: INSERT 2 rows, 1 add file, 794 bytes
    // VERSION 2: INSERT 3 rows, 1 add file, 803 bytes
    // VERSION 3: UPDATE 4 rows, 4 cdf files, 8 cdf rows, 1100+ bytes each
    //   For CDC commits we either admit the entire commit or nothing at all.
    //   This is to avoid returning `update_preimage` and `update_postimage` in separate
    //   batches.
    // VERSION 4: REMOVE 4 rows, 2 remove files, 1000+ bytes each
    Map(1 -> Seq(2, 3, 8, 2, 2), 700 -> Seq(2, 3, 8, 2, 2), 800 -> Seq(5, 8, 2, 2),
      1600 -> Seq(13, 4), 6000 -> Seq(13, 4), 7000 -> Seq(15, 2), 9000 -> Seq(17)).foreach {
      case (k, v) =>
        val query = withCDFStreamReaderAtVersion()
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

  integrationTest("maxBytesPerTrigger on CDF - ignored when using Trigger.Once") {
    val query = withCDFStreamReaderAtVersion()
      .option("maxBytesPerTrigger", "1b")
      .load().writeStream.format("console")
      .trigger(Trigger.Once)
      .start()

    try {
      assert(query.awaitTermination(streamingTimeout.toMillis))
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1) // only one trigger was run
      progress.foreach { p =>
        assert(p.numInputRows === 17)
      }
    } finally {
      query.stop()
    }
  }

  integrationTest("maxBytesPerTrigger on CDF - max bytes and max files together") {
    // should process one file at a time
    var query = withCDFStreamReaderAtVersion()
      .option(DeltaSharingOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
      .option(DeltaSharingOptions.MAX_BYTES_PER_TRIGGER_OPTION, "100gb")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 5)
      val expectedProgresses = Seq(2, 3, 8, 2, 2)
      progress.zipWithIndex.foreach { case (p, index) =>
        assert(p.numInputRows === expectedProgresses(index))
      }
    } finally {
      query.stop()
    }

    query = withCDFStreamReaderAtVersion()
      .option(DeltaSharingOptions.MAX_FILES_PER_TRIGGER_OPTION, "2")
      .option(DeltaSharingOptions.MAX_BYTES_PER_TRIGGER_OPTION, "1b")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 5)
      val expectedProgresses = Seq(2, 3, 8, 2, 2)
      progress.zipWithIndex.foreach { case (p, index) =>
        assert(p.numInputRows === expectedProgresses(index))
      }
    } finally {
      query.stop()
    }
  }
}
