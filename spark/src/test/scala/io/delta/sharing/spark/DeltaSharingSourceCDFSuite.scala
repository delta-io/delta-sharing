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

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQueryException, Trigger}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{
  DateType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.time.SpanSugar._

import io.delta.sharing.spark.TestUtils._

class DeltaSharingSourceCDFSuite extends QueryTest
  with SharedSparkSession with DeltaSharingIntegrationTest {

  import testImplicits._

  // VERSION 0: CREATE TABLE
  // VERSION 1: INSERT 3 rows, 3 add files
  // VERSION 2: REMOVE 1 row, 1 remove file
  // VERSION 3: UPDATE 1 row, 1 remove file and 1 add file
  lazy val errorTablePath1 = testProfileFile.getCanonicalPath +
      "#share8.default.cdf_table_cdf_enabled"

  // allowed to query starting from version 1
  // VERSION 1: INSERT 3 rows, 3 add files
  // VERSION 2: UPDATE 1 row, 1 cdf file
  // VERSION 3: REMOVE 1 row, 1 remove file
  lazy val partitionTablePath = testProfileFile.getCanonicalPath +
      "#share8.default.cdf_table_with_partition"

  lazy val toNullTable = testProfileFile.getCanonicalPath +
    "#share8.default.streaming_notnull_to_null"
  lazy val toNotNullTable = testProfileFile.getCanonicalPath +
    "#share8.default.streaming_cdf_null_to_notnull"

  // VERSION 1: INSERT 2 rows, 1 add file
  // VERSION 2: INSERT 3 rows, 1 add file
  // VERSION 3: UPDATE 4 rows, 4 cdf files, 8 cdf rows
  // VERSION 4: REMOVE 4 rows, 2 remove files
  lazy val cdfTablePath = testProfileFile.getCanonicalPath + "#share8.default.streaming_cdf_table"

  lazy val deltaLog = RemoteDeltaLog(cdfTablePath, forStreaming = true)

  def getSource(parameters: Map[String, String]): DeltaSharingSource = {
    val options = new DeltaSharingOptions(parameters ++ Map("readChangeFeed" -> "true"))
    DeltaSharingSource(SparkSession.active, deltaLog, options)
  }

  def withStreamReaderAtVersion(
      path: String = cdfTablePath,
      startingVersion: String = "0"): DataStreamReader = {
    spark.readStream.format("deltaSharing").option("path", path)
      .option("startingVersion", startingVersion)
      .option("ignoreDeletes", "true")
      .option("ignoreChanges", "true")
      .option("readChangeFeed", "true")
  }

  /**
   * Test latestOffset for Stream CDF
   */
  integrationTest("DeltaSharingSource.latestOffset - startingVersion 0") {
    val source = getSource(Map(
      "ignoreChanges" -> "true",
      "ignoreDeletes" -> "true",
      "startingVersion" -> "0"
    ))
    val latestOffset = source.latestOffset(null, source.getDefaultReadLimit)

    // source is using table cdfTablePath, which has 4 versions listed in the comment above.
    // When startingVersion is 0, so we expect the latestOffset to be at version 5 and index -1,
    // and is not starting version, which proceeds through existing 4 versions.
    assert(latestOffset.isInstanceOf[DeltaSharingSourceOffset])
    val offset = latestOffset.asInstanceOf[DeltaSharingSourceOffset]
    assert(offset.sourceVersion == 1)
    assert(offset.tableId == deltaLog.snapshot(Some(0)).metadata.id)
    assert(offset.tableVersion == 5)
    assert(offset.index == -1)
    assert(!offset.isStartingVersion)
  }

  integrationTest("DeltaSharingSource.latestOffset - startingVersion latest") {
    val source = getSource(Map(
      "ignoreChanges" -> "true",
      "ignoreDeletes" -> "true",
      "startingVersion" -> "latest"
    ))

    // When startingVersion is latest, it ignores all existing data, so the latestOffset is null.
    // latestOffset will be a valid offset when there's new data in the source table after the query
    // started.
    val latestOffset = source.latestOffset(null, source.getDefaultReadLimit)
    assert(latestOffset == null)
  }

  integrationTest("DeltaSharingSource.latestOffset - no startingVersion") {
    val source = getSource(Map("ignoreChanges" -> "true", "ignoreDeletes" -> "true"))
    val latestOffset = source.latestOffset(null, source.getDefaultReadLimit)

    // source is using table cdfTablePath, which has 4 versions listed in the comment above.
    // When startingVersion is not specified, it will fetch the latest snapshot (at version 4), and
    // will proceed the offset to the next version: version 5 with index -1.
    assert(latestOffset.isInstanceOf[DeltaSharingSourceOffset])
    val offset = latestOffset.asInstanceOf[DeltaSharingSourceOffset]
    assert(offset.sourceVersion == 1)
    assert(offset.tableId == deltaLog.snapshot().metadata.id)
    assert(offset.tableVersion == 5)
    assert(offset.index == -1)
    assert(!offset.isStartingVersion)
  }

  /**
   * Test schema for Stream CDF
   */
  integrationTest("DeltaSharingSource.schema - success") {
    // getBatch cannot be called without writestream
    val source = getSource(Map(
      "ignoreChanges" -> "true",
      "ignoreDeletes" -> "true",
      "startingVersion" -> "0"))
    val expectedSchema = StructType(Array(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("birthday", DateType, true),
      StructField("_commit_version", LongType, true),
      StructField("_commit_timestamp", LongType, true),
      StructField("_change_type", StringType, true)
    ))
    assert(expectedSchema == source.schema)
  }

  integrationTest("Schema isReadCompatible - no exception") {
    // Latest schema is with nullable=true, which should not fail on schema with nullable=false.
    val query = spark.readStream.format("deltaSharing")
      .option("startingVersion", "0")
      .option("readChangeFeed", "true")
      .load(toNullTable).writeStream.format("console").start()

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

  integrationTest("Schema isReadCompatible - exceptions") {
    // Latest schema has a column with nullable=false, which should fail on schema from version 0
    // which has the column with nullable=true.
    var message = intercept[StreamingQueryException] {
      val query = spark.readStream.format("deltaSharing")
        .option("startingVersion", "0")
        .option("readChangeFeed", "true")
        .load(toNotNullTable).writeStream.format("console").start()
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("Detected incompatible schema change"))

    // Latest schema has a column with nullable=false.
    // Version 1 and 2 snapshots should both return metadata from version 0 (nulltable=true).
    message = intercept[StreamingQueryException] {
      val query = spark.readStream.format("deltaSharing")
        .option("startingVersion", "1")
        .option("readChangeFeed", "true")
        .load(toNotNullTable).writeStream.format("console").start()
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("Detected incompatible schema change"))

    message = intercept[StreamingQueryException] {
      val query = spark.readStream.format("deltaSharing")
        .option("startingVersion", "2")
        .option("readChangeFeed", "true")
        .load(toNotNullTable).writeStream.format("console").start()
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("Detected incompatible schema change"))

    // But it should succeed starting from version 3.
    val query = spark.readStream.format("deltaSharing")
      .option("startingVersion", "3")
      .option("readChangeFeed", "true")
      .load(toNotNullTable).writeStream.format("console").start()

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
  }

  /**
   * Test getBatch for Stream CDF
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
    withTempDirs { (checkpointDir, outputDir) =>
      val query = withStreamReaderAtVersion()
        .load().writeStream.format("parquet")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)

      try {
        query.processAllAvailable()
        val progress = query.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 1)
        progress.foreach { p =>
          assert(p.numInputRows === 17)
        }
      } finally {
        query.stop()
      }

      val expected = Seq(
        Row("1", 1, sqlDate("2020-01-01"), 1, 1667325629000L, "insert"),
        Row("2", 2, sqlDate("2020-01-01"), 1, 1667325629000L, "insert"),
        Row("1", 1, sqlDate("2021-01-01"), 2, 1667325636000L, "insert"),
        Row("2", 2, sqlDate("2021-01-01"), 2, 1667325636000L, "insert"),
        Row("3", 3, sqlDate("2021-01-01"), 2, 1667325636000L, "insert"),
        Row("1", 1, sqlDate("2020-01-01"), 3, 1667325644000L, "update_preimage"),
        Row("1", 1, sqlDate("2020-02-02"), 3, 1667325644000L, "update_postimage"),
        Row("1", 1, sqlDate("2021-01-01"), 3, 1667325644000L, "update_preimage"),
        Row("1", 1, sqlDate("2020-02-02"), 3, 1667325644000L, "update_postimage"),
        Row("2", 2, sqlDate("2020-01-01"), 3, 1667325644000L, "update_preimage"),
        Row("2", 2, sqlDate("2020-02-02"), 3, 1667325644000L, "update_postimage"),
        Row("2", 2, sqlDate("2021-01-01"), 3, 1667325644000L, "update_preimage"),
        Row("2", 2, sqlDate("2020-02-02"), 3, 1667325644000L, "update_postimage"),
        Row("1", 1, sqlDate("2020-02-02"), 4, 1667325812000L, "delete"),
        Row("1", 1, sqlDate("2020-02-02"), 4, 1667325812000L, "delete"),
        Row("2", 2, sqlDate("2020-02-02"), 4, 1667325812000L, "delete"),
        Row("2", 2, sqlDate("2020-02-02"), 4, 1667325812000L, "delete")
      )
      checkAnswer(spark.read.format("parquet").load(outputDir.getCanonicalPath), expected)
    }

    // Test readchangeData=true, and startingVersion=1.
    val query = spark.readStream.format("deltaSharing")
      .option("readchangeData", "true")
      .option("startingVersion", "1")
      .load(cdfTablePath).writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 17)
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
        assert(p.numInputRows === 17)
      }
    } finally {
      query.stop()
    }

    // select a few columns
    withTempDirs { (checkpointDir, outputDir) =>
      val query = withStreamReaderAtVersion()
        .load()
        .select("birthday", "_commit_version", "age", "_change_type")
        .writeStream.format("parquet")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)
      try {
        query.processAllAvailable()
        val progress = query.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 1)
        progress.foreach { p =>
          assert(p.numInputRows === 17)
        }
      } finally {
        query.stop()
      }

      val expected = Seq(
        Row(sqlDate("2020-01-01"), 1, 1, "insert"),
        Row(sqlDate("2020-01-01"), 1, 2, "insert"),
        Row(sqlDate("2021-01-01"), 2, 1, "insert"),
        Row(sqlDate("2021-01-01"), 2, 2, "insert"),
        Row(sqlDate("2021-01-01"), 2, 3, "insert"),
        Row(sqlDate("2020-01-01"), 3, 1, "update_preimage"),
        Row(sqlDate("2020-02-02"), 3, 1, "update_postimage"),
        Row(sqlDate("2021-01-01"), 3, 1, "update_preimage"),
        Row(sqlDate("2020-02-02"), 3, 1, "update_postimage"),
        Row(sqlDate("2020-01-01"), 3, 2, "update_preimage"),
        Row(sqlDate("2020-02-02"), 3, 2, "update_postimage"),
        Row(sqlDate("2021-01-01"), 3, 2, "update_preimage"),
        Row(sqlDate("2020-02-02"), 3, 2, "update_postimage"),
        Row(sqlDate("2020-02-02"), 4, 1, "delete"),
        Row(sqlDate("2020-02-02"), 4, 1, "delete"),
        Row(sqlDate("2020-02-02"), 4, 2, "delete"),
        Row(sqlDate("2020-02-02"), 4, 2, "delete")
      )
      checkAnswer(spark.read.format("parquet").load(outputDir.getCanonicalPath), expected)
    }
  }

  integrationTest("CDF Stream filter - success") {
    // filter on birthday
    withTempDirs { (checkpointDir, outputDir) =>
      val query = withStreamReaderAtVersion(path = partitionTablePath, startingVersion = "1")
        .load()
        .filter($"birthday" === "2020-01-01" && $"age" === 2)
        .writeStream.format("parquet")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)
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

      val expected = Seq(
        Row("2", 2, sqlDate("2020-01-01"), 1L, 1651614980000L, "insert"),
        Row("2", 2, sqlDate("2020-01-01"), 2L, 1651614986000L, "update_preimage")
      )
      checkAnswer(spark.read.format("parquet").load(outputDir.getCanonicalPath), expected)
    }

    // filter on added cdf columns
    withTempDirs { (checkpointDir, outputDir) =>
      val query = withStreamReaderAtVersion(path = partitionTablePath, startingVersion = "1")
        .load()
        .filter(col("_change_type").contains("delete"))
        .writeStream.format("parquet")
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
        Row("3", 3, sqlDate("2020-03-03"), 3L, 1651614994000L, "delete")
      )
      checkAnswer(spark.read.format("parquet").load(outputDir.getCanonicalPath), expected)
    }
  }

  integrationTest("CDF Stream - exceptions") {
    // For errorTablePath1(cdf_table_cdf_enabled), cdf is disabled at version 4, so there will
    // exception returned from the server.
    var query = withStreamReaderAtVersion(path = errorTablePath1)
      .load().writeStream.format("console").start()
    var message = intercept[StreamingQueryException] {
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("Error getting change data for range"))

    query = withStreamReaderAtVersion(path = partitionTablePath, startingVersion = "0")
      .load().writeStream.format("console").start()
    message = intercept[StreamingQueryException] {
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("You can only query table changes since version 1."))

    // startingTimestamp > latest version
    message = intercept[StreamingQueryException] {
      val query = spark.readStream.format("deltaSharing")
        .option("readchangeFeed", "true")
        .option("startingTimestamp", "9999-01-01 00:00:00.0")
        .load(cdfTablePath).writeStream.format("console").start()
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("The provided timestamp "))

    // startingTimestamp corresponds to version 0, < starting version 1 on partitionTablePath.
    message = intercept[StreamingQueryException] {
      val query = spark.readStream.format("deltaSharing")
        .option("readchangeFeed", "true")
        .option("startingTimestamp", "2022-01-01 00:00:00")
        .load(partitionTablePath).writeStream.format("console").start()
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("The provided timestamp"))
    assert(message.contains("corresponds to 0"))
    // startingVersion > latest version
  }

  integrationTest("CDF Stream - different startingVersion") {
    // Starting from version 3: 4 ADDCDCFiles
    var query = withStreamReaderAtVersion(startingVersion = "3")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 12)
      }
    } finally {
      query.stop()
    }

    // Starting from version 4: 2 RemoveFiles
    query = withStreamReaderAtVersion(startingVersion = "4")
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

    // no startingVersion - should result fetch the latest snapshot, which has only 1 row
    withTempDirs { (checkpointDir, outputDir) =>
      val query = spark.readStream.format("deltaSharing")
        .option("readchangeFeed", "true")
        .load(cdfTablePath).writeStream.format("parquet")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)

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

      val expected = Seq(
        Row("3", 3, sqlDate("2021-01-01"), 4, 1667325812000L, "insert")
      )
      checkAnswer(spark.read.format("parquet").load(outputDir.getCanonicalPath), expected)
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
    val query = spark.readStream.format("deltaSharing")
      .option("readchangeFeed", "true")
      .option("startingTimestamp", "2022-01-01 00:00:00")
      .load(cdfTablePath).writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 17)
      }
    } finally {
      query.stop()
    }
  }

  /**
   * Test streaming checkpoint
   */
  integrationTest("CDF Stream - restart from checkpoint") {
    withTempDirs { (checkpointDir, outputDir) =>
      val processedRows = Seq(2, 3, 8, 2, 2)
      val query = withStreamReaderAtVersion()
        .option("maxFilesPerTrigger", "1")
        .load().select("age").writeStream.format("parquet")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)
      try {
        query.processAllAvailable()
        val progress = query.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === processedRows.size)
        progress.zipWithIndex.map { case (p, index) =>
          assert(p.numInputRows === processedRows(index))
        }
      } finally {
        query.stop()
      }

      val restartQuery = withStreamReaderAtVersion()
        .option("maxFilesPerTrigger", "1")
        .load().select("age").writeStream.format("console")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start()
      // There are 5 checkpoints for 5 getBatch(), which is caused by maxFilesPerTrigger = 1,
      // remove the latest 3 checkpoints.
      val checkpointFiles = FileUtils.listFiles(checkpointDir, null, true).asScala
      checkpointFiles.foreach{ f =>
        if (!f.isDirectory() && (f.getCanonicalPath.endsWith("2") ||
          f.getCanonicalPath.endsWith("3") || f.getCanonicalPath.endsWith("4") ||
          f.getCanonicalPath.endsWith("2.crc") || f.getCanonicalPath.endsWith("3.crc") ||
          f.getCanonicalPath.endsWith("4.crc") || f.getCanonicalPath.endsWith("metadata.crc"))) {
          f.delete()
        }
        // SparkStructuredStreaming requires query id match to reuse a checkpoint.
        if (f.getCanonicalPath.endsWith("metadata")) {
          FileUtils.writeStringToFile(f, s"""{"id":"${restartQuery.id}"}""")
        }
      }

      try {
        restartQuery.processAllAvailable()
        val progress = restartQuery.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === processedRows.size - 2)
        progress.zipWithIndex.map { case (p, index) =>
          assert(p.numInputRows === processedRows(index + 2))
        }
      } finally {
        restartQuery.stop()
      }
    }
  }

  /**
   * Test maxVersionsPerRpc
   */
  integrationTest("maxVersionsPerRpc - success") {
    // VERSION 1: INSERT 2 rows, 1 add file
    // VERSION 2: INSERT 3 rows, 1 add file
    // VERSION 3: UPDATE 4 rows, 4 cdf files, 8 cdf rows
    // VERSION 4: REMOVE 4 rows, 2 remove files
    // maxVersionsPerRpc = 1
    var processedRows = Seq(2, 3, 8, 4)
    var query = withStreamReaderAtVersion()
      .option("maxVersionsPerRpc", "1")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === processedRows.size)
      progress.zipWithIndex.map { case (p, index) =>
        assert(p.numInputRows === processedRows(index))
      }
    } finally {
      query.stop()
    }

    // maxVersionsPerRpc = 2
    processedRows = Seq(2, 11, 4)
    query = withStreamReaderAtVersion()
      .option("maxVersionsPerRpc", "2")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === processedRows.size)
      progress.zipWithIndex.map { case (p, index) =>
        assert(p.numInputRows === processedRows(index))
      }
    } finally {
      query.stop()
    }

    // maxVersionsPerRpc = 3
    processedRows = Seq(5, 12)
    query = withStreamReaderAtVersion()
      .option("maxVersionsPerRpc", "3")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === processedRows.size)
      progress.zipWithIndex.map { case (p, index) =>
        assert(p.numInputRows === processedRows(index))
      }
    } finally {
      query.stop()
    }

    // maxVersionsPerRpc = 4
    processedRows = Seq(13, 4)
    query = withStreamReaderAtVersion()
      .option("maxVersionsPerRpc", "4")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === processedRows.size)
      progress.zipWithIndex.map { case (p, index) =>
        assert(p.numInputRows === processedRows(index))
      }
    } finally {
      query.stop()
    }

    // maxVersionsPerRpc = 5
    processedRows = Seq(17)
    query = withStreamReaderAtVersion()
      .option("maxVersionsPerRpc", "5")
      .load().writeStream.format("console").start()
    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === processedRows.size)
      progress.zipWithIndex.map { case (p, index) =>
        assert(p.numInputRows === processedRows(index))
      }
    } finally {
      query.stop()
    }
  }

}
