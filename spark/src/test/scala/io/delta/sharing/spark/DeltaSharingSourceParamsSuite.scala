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

import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQueryException}
import org.apache.spark.sql.test.SharedSparkSession

import io.delta.sharing.spark.TestUtils._

class DeltaSharingSourceParamsSuite extends QueryTest
  with SharedSparkSession with DeltaSharingIntegrationTest {

  import testImplicits._

  // VERSION 0: CREATE TABLE
  // VERSION 1: INSERT 3 rows, 3 add files
  // VERSION 2: REMOVE 1 row, 1 remove file
  // VERSION 3: UPDATE 1 row, 1 remove file and 1 add file
  lazy val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_cdf_enabled"

  lazy val deltaLog = RemoteDeltaLog(tablePath, forStreaming = true)

  def getSource(parameters: Map[String, String]): DeltaSharingSource = {
    val options = new DeltaSharingOptions(parameters)
    DeltaSharingSource(SparkSession.active, deltaLog, options)
  }

  def withStreamReaderAtVersion(
      path: String = tablePath,
      startingVersion: String = "0"): DataStreamReader = {
    spark.readStream.format("deltaSharing").option("path", path)
      .option("startingVersion", startingVersion)
      .option("ignoreDeletes", "true")
      .option("ignoreChanges", "true")
  }

  def withSkipChangeCommitsStreamReaderAtVersion(
      path: String = tablePath,
      startingVersion: String = "0"): DataStreamReader = {
    spark.readStream.format("deltaSharing").option("path", path)
      .option("startingVersion", startingVersion)
      .option("skipChangeCommits", "true")
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
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("Detected deleted data from streaming source at version 2"))

    // There are updates at version 3 of cdf_table_cdf_enabled
    query = spark.readStream.format("deltaSharing").option("path", tablePath)
      .option("startingVersion", "0")
      .option("ignoreDeletes", "true")
      .load().writeStream.format("console").start()
    message = intercept[StreamingQueryException] {
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("Detected a data update in the source table at version 3"))
    assert(message.contains("If you'd like to ignore updates, set the option 'skipChangeCommits'" +
      " to 'true'."))
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

    var message = intercept[IllegalArgumentException] {
      val query = spark.readStream.format("deltaSharing").option("path", tablePath)
        .option("startingTimestamp", "")
        .load().writeStream.format("console").start()
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("The provided timestamp () cannot be converted to a valid timestamp"))

    message = intercept[IllegalArgumentException] {
      val query = spark.readStream.format("deltaSharing").option("path", tablePath)
        .option("startingTimestamp", "2022-01-01")
        .option("startingVersion", "1")
        .load().writeStream.format("console").start()
    }.getMessage
    assert(message.contains("Please either provide 'startingVersion' or 'startingTimestamp'."))

    message = intercept[StreamingQueryException] {
      val query = spark.readStream.format("deltaSharing").option("path", tablePath)
        .option("startingTimestamp", "9999-01-01 00:00:00.0")
        .load().writeStream.format("console").start()
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("The provided timestamp "))
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
    var query = withStreamReaderAtVersion(startingVersion = "1")
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

    query = withStreamReaderAtVersion(startingVersion = "2")
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

    // there are 3 versions in total
    query = withStreamReaderAtVersion(startingVersion = "4")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 0)
    } finally {
      query.stop()
    }
  }

  /**
   * Test basic streaming functionality with 'skipChangeCommits' as true.
   */
  integrationTest("skipChangeCommits - basic - success") {
    val query = withSkipChangeCommitsStreamReaderAtVersion()
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
  }

  integrationTest("skipChangeCommits - basic memory - success") {
    val query = withSkipChangeCommitsStreamReaderAtVersion()
      .load().writeStream.format("memory").queryName("streamMemoryOutput").start()

    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 3)
      }

      val expected = Seq(
        Row("3", 3, sqlDate("2020-01-01")),
        Row("2", 2, sqlDate("2020-01-01")),
        Row("1", 1, sqlDate("2020-01-01"))
      )
      checkAnswer(sql("SELECT * FROM streamMemoryOutput"), expected)
    } finally {
      query.stop()
    }
  }

  integrationTest("skipChangeCommits - outputDataframe - success") {
    withTempDirs { (checkpointDir, outputDir) =>
      val query = withSkipChangeCommitsStreamReaderAtVersion()
        .load().writeStream.format("parquet")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)

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

      val expected = Seq(
        Row("3", 3, sqlDate("2020-01-01")),
        Row("2", 2, sqlDate("2020-01-01")),
        Row("1", 1, sqlDate("2020-01-01"))
      )
      checkAnswer(spark.read.format("parquet").load(outputDir.getCanonicalPath), expected)
    }
  }

  integrationTest("skipChangeCommits - no startingVersion - success") {
    // cdf_table_cdf_enabled snapshot at version 5 is queried, with 2 files and 2 rows of data
    val query = spark.readStream.format("deltaSharing")
      .option("skipChangeCommits", "true")
      .load(tablePath)
      .select("birthday", "name", "age").writeStream.format("console").start()

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
}
