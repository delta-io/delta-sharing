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
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession}
import org.apache.spark.sql.connector.read.streaming.ReadMaxFiles
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQueryException, Trigger}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{
  DateType,
  IntegerType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.scalatest.time.SpanSugar._

import io.delta.sharing.spark.TestUtils._

class DeltaSharingSourceSuite extends QueryTest
  with SharedSparkSession with DeltaSharingIntegrationTest {

  // TODO: test with different Trigger.xx:
  //   https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

  import testImplicits._

  // VERSION 0: CREATE TABLE
  // VERSION 1: INSERT 3 rows, 3 add files
  // VERSION 2: REMOVE 1 row, 1 remove file
  // VERSION 3: UPDATE 1 row, 1 remove file and 1 add file
  lazy val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_cdf_enabled"

  // allowed to query starting from version 1
  // VERSION 1: INSERT 3 rows, 3 add files
  // VERSION 2: UPDATE 1 row, 1 cdf file
  // VERSION 3: REMOVE 1 row, 1 remove file
  lazy val partitionTablePath = testProfileFile.getCanonicalPath +
    "#share8.default.cdf_table_with_partition"

  // VERSION 1: INSERT 2 rows, 1 add file
  // VERSION 2: INSERT 3 rows, 1 add file
  // VERSION 3: UPDATE 4 rows, 3 add files, 2 remove files, 5 rows
  // VERSION 4: REMOVE 4 rows, 2 remove files
  lazy val cdfTablePath = testProfileFile.getCanonicalPath + "#share8.default.streaming_cdf_table"

  lazy val toNullTable = testProfileFile.getCanonicalPath +
      "#share8.default.streaming_notnull_to_null"
  lazy val toNotNullTable = testProfileFile.getCanonicalPath +
    "#share8.default.streaming_null_to_notnull"

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
    assert(offset.tableVersion == 6)
    assert(offset.index == -1)
    assert(!offset.isStartingVersion)
  }

  integrationTest("DeltaSharingSource.latestOffset - startingVersion latest") {
    val source = getSource(Map(
      "ignoreChanges" -> "true",
      "ignoreDeletes" -> "true",
      "startingVersion" -> "latest"
    ))
    // #share8.default.cdf_table_cdf_enabled_<yyyyMMdd_HHmmss>_<UUID>
    assert(source.deltaLog.path.toString.split("_").size == 7)
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
   * Test schema for Stream
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
      StructField("birthday", DateType, true)
    ))
    assert(expectedSchema == source.schema)
  }

  integrationTest("disallow user specified schema") {
    val message = intercept[UnsupportedOperationException] {
      spark.readStream.format("deltaSharing")
        .schema(StructType(Array(StructField("a", TimestampType), StructField("b", StringType))))
        .load(tablePath)
    }.getMessage
    assert(message.contains("Delta sharing does not support specifying the schema at read time"))
  }

  integrationTest("Schema isReadCompatible - no exception") {
    // Latest schema is with nullable=true, which should not fail on schema with nullable=false.
    val query = spark.readStream.format("deltaSharing")
      .option("startingVersion", "0")
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
    // Latest schema is with nullable=false, which should fail on schema with nullable=true.
    var message = intercept[StreamingQueryException] {
      val query = spark.readStream.format("deltaSharing")
        .option("startingVersion", "0")
        .load(toNotNullTable).writeStream.format("console").start()
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("Detected incompatible schema change"))

    // Version 1 and 2 should both return metadata from version 0, which raises the same error.
    message = intercept[StreamingQueryException] {
      val query = spark.readStream.format("deltaSharing")
        .option("startingVersion", "1")
        .load(toNotNullTable).writeStream.format("console").start()
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("Detected incompatible schema change"))

    message = intercept[StreamingQueryException] {
      val query = spark.readStream.format("deltaSharing")
        .option("startingVersion", "2")
        .load(toNotNullTable).writeStream.format("console").start()
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("Detected incompatible schema change"))

    // But it should succeed starting from version 3.
    val query = spark.readStream.format("deltaSharing")
      .option("startingVersion", "3")
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
   * Test spark config of query table version interval
   */
  integrationTest("query table version interval cannot be less than 30 seconds") {
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds",
      "29"
    )
    val message = intercept[Exception] {
      val query = spark.readStream.format("deltaSharing").load(tablePath)
        .writeStream.format("console").start()
      query.processAllAvailable()
    }.getMessage
    assert(message.contains("must not be less than 30 seconds."))
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds",
      "30"
    )
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

  integrationTest("basic memory - success") {
    val query = withStreamReaderAtVersion()
      .load().writeStream.format("memory").queryName("streamMemoryOutput").start()

    try {
      query.processAllAvailable()
      val progress = query.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 1)
      progress.foreach { p =>
        assert(p.numInputRows === 4)
      }

      val expected = Seq(
        Row("2", 2, sqlDate("2020-01-01")),
        Row("3", 3, sqlDate("2020-01-01")),
        Row("2", 2, sqlDate("2020-02-02")),
        Row("1", 1, sqlDate("2020-01-01"))
      )
      checkAnswer(sql("SELECT * FROM streamMemoryOutput"), expected)
    } finally {
      query.stop()
    }
  }

  integrationTest("outputDataframe - success") {
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
          assert(p.numInputRows === 4)
        }
      } finally {
        query.stop()
      }

      val expected = Seq(
        Row("2", 2, sqlDate("2020-01-01")),
        Row("3", 3, sqlDate("2020-01-01")),
        Row("2", 2, sqlDate("2020-02-02")),
        Row("1", 1, sqlDate("2020-01-01"))
      )
      checkAnswer(spark.read.format("parquet").load(outputDir.getCanonicalPath), expected)
    }
  }

  integrationTest("no startingVersion - success") {
    // cdf_table_cdf_enabled snapshot at version 5 is queried, with 2 files and 2 rows of data
    val query = spark.readStream.format("deltaSharing")
      .option("ignoreDeletes", "true")
      .option("ignoreChanges", "true")
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

  integrationTest("Stream filter - success") {
    // no filters
    withTempDirs { (checkpointDir, outputDir) =>
      val query = withStreamReaderAtVersion(path = partitionTablePath, startingVersion = "1")
        .load()
        .writeStream.format("parquet")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)
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

      val expected = Seq(
        Row("1", 1, sqlDate("2020-01-01")),
        Row("2", 2, sqlDate("2020-01-01")),
        Row("3", 3, sqlDate("2020-03-03")),
        Row("2", 2, sqlDate("2020-02-02"))
      )
      checkAnswer(spark.read.format("parquet").load(outputDir.getCanonicalPath), expected)
    }

    // filter on birthday and age
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
          assert(p.numInputRows === 1)
        }
      } finally {
        query.stop()
      }

      val expected = Seq(
        Row("2", 2, sqlDate("2020-01-01"))
      )
      checkAnswer(spark.read.format("parquet").load(outputDir.getCanonicalPath), expected)
    }
  }

  /**
   * Test Trigger.ProcessingTime
   */
  integrationTest("Trigger.ProcessingTime - success") {
    withTempDirs { (checkpointDir, outputDir) =>
      val query = withStreamReaderAtVersion()
        .option("maxFilesPerTrigger", "1")
        .load().writeStream.format("parquet")
        // This is verified by Console output
        .trigger(Trigger.ProcessingTime("20 seconds"))
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)

      try {
        query.processAllAvailable()
        val progress = query.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 4)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
      } finally {
        query.stop()
      }
    }
  }

  integrationTest("restart from checkpoint - success") {
    withTempDirs { (checkpointDir, outputDir) =>
      val query = withStreamReaderAtVersion()
        .option("maxFilesPerTrigger", "1")
        .load().writeStream.format("parquet")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)
      try {
        query.processAllAvailable()
        val progress = query.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 4)
        progress.foreach {
          p => assert(p.numInputRows === 1)
        }
      } finally {
        query.stop()
      }

      // Verify the output dataframe
      val expected = Seq(
        Row("2", 2, sqlDate("2020-01-01")),
        Row("3", 3, sqlDate("2020-01-01")),
        Row("2", 2, sqlDate("2020-02-02")),
        Row("1", 1, sqlDate("2020-01-01"))
      )
      checkAnswer(spark.read.format("parquet").load(outputDir.getCanonicalPath), expected)

      val newQuery = withStreamReaderAtVersion()
        .option("maxFilesPerTrigger", "1")
        .load().writeStream.format("console")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start()

      // There are 4 checkpoints, remove the latest 2.
      val checkpointFiles = FileUtils.listFiles(checkpointDir, null, true).asScala
      checkpointFiles.foreach{ f =>
        if (!f.isDirectory() &&
          (f.getCanonicalPath.endsWith("2") || f.getCanonicalPath.endsWith("3") ||
            f.getCanonicalPath.endsWith("2.crc") || f.getCanonicalPath.endsWith("3.crc") ||
            f.getCanonicalPath.endsWith("metadata.crc"))) {
          f.delete()
        }
        // The metadata contains a single line of json, which is the queryid:
        //   {"id":"aaaabbbb-cccc-dddd-eeee-ffffgggghhhh"}
        // SparkStructuredStreaming requires query id match to reuse a checkpoint.
        if (f.getCanonicalPath.endsWith("metadata")) {
          FileUtils.writeStringToFile(f, s"""{"id":"${newQuery.id}"}""")
        }
      }

      try {
        newQuery.processAllAvailable()
        val progress = newQuery.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 2)
        progress.foreach {
          p => assert(p.numInputRows === 1)
        }
      } finally {
        newQuery.stop()
      }
    }
  }

  /**
   * Test maxVersionsPerRpc
   */
  integrationTest("maxVersionsPerRpc - success") {
    // VERSION 1: INSERT 2 rows, 1 add file
    // VERSION 2: INSERT 3 rows, 1 add file
    // VERSION 3: UPDATE 4 rows, 4 cdf files, 4 new rows
    // VERSION 4: REMOVE 4 rows, 2 remove files, no new rows

    // maxVersionsPerRpc = 1
    var processedRows = Seq(2, 3, 5)
    var query = withStreamReaderAtVersion(path = cdfTablePath)
      .option("maxVersionsPerRpc", "1")
      .load()
      .writeStream.format("console").start()
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
    processedRows = Seq(2, 8)
    query = withStreamReaderAtVersion(path = cdfTablePath)
      .option("maxVersionsPerRpc", "2")
      .load()
      .writeStream.format("console").start()
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
    processedRows = Seq(5, 5)
    query = withStreamReaderAtVersion(path = cdfTablePath)
      .option("maxVersionsPerRpc", "3")
      .load()
      .writeStream.format("console").start()
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
    processedRows = Seq(10)
    query = withStreamReaderAtVersion(path = cdfTablePath)
      .option("maxVersionsPerRpc", "4")
      .load()
      .writeStream.format("console").start()
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
