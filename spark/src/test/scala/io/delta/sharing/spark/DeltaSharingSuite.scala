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

import java.io.EOFException

import scala.util.Random

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

class DeltaSharingSuite extends QueryTest with SharedSparkSession with DeltaSharingIntegrationTest {

  import testImplicits._

  protected def sqlDate(date: String): java.sql.Date = {
    toJavaDate(stringToDate(
      UTF8String.fromString(date),
      getZoneId(SQLConf.get.sessionLocalTimeZone)).get)
  }

  protected def sqlTimestamp(timestamp: String): java.sql.Timestamp = {
    toJavaTimestamp(stringToTimestamp(
      UTF8String.fromString(timestamp),
      getZoneId(SQLConf.get.sessionLocalTimeZone)).get)
  }

  integrationTest("table1") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.table1"
    val expected = Seq(
      Row(sqlTimestamp("2021-04-27 23:32:02.07"), sqlDate("2021-04-28")),
      Row(sqlTimestamp("2021-04-27 23:32:22.421"), sqlDate("2021-04-28"))
    )
    checkAnswer(spark.read.format("deltaSharing").load(tablePath), expected)
    withTable("delta_sharing_test") {
      sql(s"CREATE TABLE delta_sharing_test USING deltaSharing LOCATION '$tablePath'")
      checkAnswer(sql(s"SELECT * FROM delta_sharing_test"), expected)
    }
  }

  integrationTest("table2") {
    val tablePath = testProfileFile.getCanonicalPath + "#share2.default.table2"
    val expected = Seq(
      Row(sqlTimestamp("2021-04-28 16:33:57.955"), sqlDate("2021-04-28")),
      Row(sqlTimestamp("2021-04-28 16:33:48.719"), sqlDate("2021-04-28"))
    )
    checkAnswer(spark.read.format("deltaSharing").load(tablePath), expected)
    withTable("delta_sharing_test") {
      sql(s"CREATE TABLE delta_sharing_test USING deltaSharing LOCATION '$tablePath'")
      checkAnswer(sql(s"SELECT * FROM delta_sharing_test"), expected)
    }
  }

  integrationTest("table3") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.table3"
    val expected = Seq(
      Row(sqlTimestamp("2021-04-28 16:35:53.156"), sqlDate("2021-04-28"), null),
      Row(sqlTimestamp("2021-04-28 16:36:47.599"), sqlDate("2021-04-28"), "foo"),
      Row(sqlTimestamp("2021-04-28 16:36:51.945"), sqlDate("2021-04-28"), "bar")
    )
    checkAnswer(spark.read.format("deltaSharing").load(tablePath), expected)
    withTable("delta_sharing_test") {
      sql(s"CREATE TABLE delta_sharing_test USING deltaSharing LOCATION '$tablePath'")
      checkAnswer(sql(s"SELECT * FROM delta_sharing_test"), expected)
    }
  }

  integrationTest("table4: table column order is not the same as parquet files") {
    val tablePath = testProfileFile.getCanonicalPath + "#share3.default.table4"
    val expected = Seq(
      Row(null, sqlTimestamp("2021-04-28 16:33:57.955"), sqlDate("2021-04-28")),
      Row(null, sqlTimestamp("2021-04-28 16:33:48.719"), sqlDate("2021-04-28"))
    )
    checkAnswer(spark.read.format("deltaSharing").load(tablePath), expected)
    withTable("delta_sharing_test") {
      sql(s"CREATE TABLE delta_sharing_test USING deltaSharing LOCATION '$tablePath'")
      checkAnswer(sql(s"SELECT * FROM delta_sharing_test"), expected)
    }
  }

  integrationTest("table5: empty table") {
    val tablePath = testProfileFile.getCanonicalPath + "#share3.default.table5"
    checkAnswer(spark.read.format("deltaSharing").load(tablePath), Nil)
    val expectedSchema = StructType(Array(
      StructField("eventTime", TimestampType),
      StructField("date", DateType),
      StructField("type", StringType).withComment("this is a comment")))
    assert(spark.read.format("deltaSharing").load(tablePath).schema == expectedSchema)
    withTable("delta_sharing_test") {
      sql(s"CREATE TABLE delta_sharing_test USING deltaSharing LOCATION '$tablePath'")
      checkAnswer(sql(s"SELECT * FROM delta_sharing_test"), Nil)
      assert(sql(s"SELECT * FROM delta_sharing_test").schema == expectedSchema)
    }
  }

  integrationTest("cdf_table_cdf_enabled query without version") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_cdf_enabled"
    val expected = Seq(
      Row("1", 1, sqlDate("2020-01-01")),
      Row("2", 2, sqlDate("2020-02-02"))
    )
    checkAnswer(spark.read.format("deltaSharing").load(tablePath), expected)
    withTable("delta_sharing_test") {
      sql(s"CREATE TABLE delta_sharing_test USING deltaSharing LOCATION '$tablePath'")
      checkAnswer(sql(s"SELECT * FROM delta_sharing_test"), expected)
    }
  }

  integrationTest("cdf_table_cdf_enabled query with valid version") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_cdf_enabled"
    val expected = Seq(
      Row("1", 1, sqlDate("2020-01-01")),
      Row("2", 2, sqlDate("2020-01-01")),
      Row("3", 3, sqlDate("2020-01-01"))
    )
    checkAnswer(
      spark.read.format("deltaSharing").option("versionAsOf", 1).load(tablePath),
      expected
    )
  }

  integrationTest("cdf_table_cdf_enabled version exception") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_cdf_enabled"
    val expected = Seq()
    val errorMessage = intercept[IllegalArgumentException] {
      checkAnswer(
        spark.read.format("deltaSharing").option("versionAsOf", "3x").load(tablePath), expected)
    }.getMessage
    assert(errorMessage.contains("Invalid value '3x' for option 'versionAsOf'"))
  }

  integrationTest("cdf_table_cdf_enabled timestamp exception") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_cdf_enabled"
    val expected = Seq()
    var errorMessage = intercept[io.delta.sharing.spark.util.UnexpectedHttpStatus] {
      checkAnswer(
        spark.read
          .format("deltaSharing")
          .option("timestampAsOf", "2000-01-01 00:00:00")
          .load(tablePath),
        expected
      )
    }.getMessage
    assert(errorMessage.contains("The provided timestamp (2000-01-01 00:00:00.0) is before"))

    errorMessage = intercept[IllegalArgumentException] {
      checkAnswer(
        spark.read
          .format("deltaSharing")
          .option("versionAsOf", "3")
          .option("timestampAsOf", "2000-01-01 00:00:00")
          .load(tablePath),
        expected
      )
    }.getMessage
    assert(errorMessage.contains("Please either provide 'versionAsOf' or 'timestampAsOf'"))
  }

  integrationTest("test_gzip: non-default compression codec") {
    val tablePath = testProfileFile.getCanonicalPath + "#share4.default.test_gzip"
    val expected = Seq(Row(true, 1, "Hi"))
    checkAnswer(spark.read.format("deltaSharing").load(tablePath), expected)
    withTable("delta_sharing_test") {
      sql(s"CREATE TABLE delta_sharing_test USING deltaSharing LOCATION '$tablePath'")
      checkAnswer(sql(s"SELECT * FROM delta_sharing_test"), expected)
    }
  }

  integrationTest("partition pruning") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.table3"
    val expected = Seq(
      Row(sqlTimestamp("2021-04-28 16:35:53.156"), sqlDate("2021-04-28"), null),
      Row(sqlTimestamp("2021-04-28 16:36:47.599"), sqlDate("2021-04-28"), "foo"),
      Row(sqlTimestamp("2021-04-28 16:36:51.945"), sqlDate("2021-04-28"), "bar")
    )
    checkAnswer(
      spark.read.format("deltaSharing").load(tablePath).where("date = '2021-04-28'"), expected)
    withTable("delta_sharing_test") {
      sql(s"CREATE TABLE delta_sharing_test USING deltaSharing LOCATION '$tablePath'")
      checkAnswer(sql(s"SELECT * FROM delta_sharing_test WHERE date = '2021-04-28'"), expected)
    }
  }

  integrationTest("table_changes: cdf_table_cdf_enabled") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_cdf_enabled"

    val expected = Seq(
      Row("1", 1, sqlDate("2020-01-01"), 1L, 1651272635000L, "insert"),
      Row("2", 2, sqlDate("2020-01-01"), 1L, 1651272635000L, "insert"),
      Row("3", 3, sqlDate("2020-01-01"), 1L, 1651272635000L, "insert"),
      Row("2", 2, sqlDate("2020-01-01"), 3L, 1651272660000L, "update_preimage"),
      Row("2", 2, sqlDate("2020-02-02"), 3L, 1651272660000L, "update_postimage"),
      Row("3", 3, sqlDate("2020-01-01"), 2L, 1651272655000L, "delete")
    )
    val result = spark.read.format("deltaSharing")
      .option("readChangeFeed", "true")
      .option("startingVersion", 0)
      .option("endingVersion", 3).load(tablePath)
    checkAnswer(result, expected)

    // should work when selecting some columns in a different order
    checkAnswer(
      result.select("_change_type", "birthday", "age"),
      Seq(
        Row("insert", sqlDate("2020-01-01"), 1),
        Row("insert", sqlDate("2020-01-01"), 2),
        Row("insert", sqlDate("2020-01-01"), 3),
        Row("update_preimage", sqlDate("2020-01-01"), 2),
        Row("update_postimage", sqlDate("2020-02-02"), 2),
        Row("delete", sqlDate("2020-01-01"), 3)
      )
    )
  }

  integrationTest("table_changes_empty: cdf_table_cdf_enabled") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_cdf_enabled"

    val result = spark.read.format("deltaSharing")
      .option("readChangeFeed", "true")
      .option("startingVersion", 5).load(tablePath)
    checkAnswer(result, Seq.empty)
  }

  integrationTest("table_changes_with_timestamp: cdf_table_cdf_enabled") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_cdf_enabled"

    // Use a start timestamp in the past, and expect an error.
    val result1 = intercept[IllegalStateException] {
      val df = spark.read.format("deltaSharing")
        .option("readChangeFeed", "true")
        .option("startingTimestamp", "2000-01-01 00:00:00").load(tablePath)
      checkAnswer(df, Nil)
    }
    assert (result1.getMessage.contains("Please use a timestamp greater"))

    // Use an end timestamp in the future, and expect an error.
    val result2 = intercept[IllegalStateException] {
      val df = spark.read.format("deltaSharing")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .option("endingTimestamp", "2100-01-01 00:00:00").load(tablePath)
      checkAnswer(df, Nil)
    }
    assert (result2.getMessage.contains("Please use a timestamp less"))
  }

  integrationTest("table_changes: cdf_table_with_vacuum") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_with_vacuum"

    val expected = Seq(
      Row(11, 2L, 1655410824000L, "update_preimage"),
      Row(21, 2L, 1655410824000L, "update_postimage"),
      Row(31, 3L, 1655410829000L, "insert"),
      Row(31, 4L, 1655410847000L, "delete"),
      Row(32, 3L, 1655410829000L, "insert")
    )
    val result = spark.read.format("deltaSharing")
      .option("readChangeFeed", "true")
      .option("startingVersion", 2)
      .option("endingVersion", 4).load(tablePath)
    checkAnswer(result, expected)
  }

  integrationTest("table_changes_exception: cdf_table_with_vacuum") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_with_vacuum"

    // parquet file is vacuumed, will see 404 error when requsting the presigned url
    val ex = intercept[org.apache.spark.SparkException] {
      val df = spark.read.format("deltaSharing")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0).load(tablePath)
      checkAnswer(df, Nil)
    }
    assert (ex.getMessage.contains("404 Not Found"))
    assert (ex.getMessage.contains("c000.snappy.parquet"))
  }

  integrationTest("table_changes_exception: cdf_table_missing_log") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_missing_log"

    // log file is missing
    val ex = intercept[io.delta.sharing.spark.util.UnexpectedHttpStatus] {
      val df = spark.read.format("deltaSharing")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0).load(tablePath)
      checkAnswer(df, Nil)
    }
    assert (ex.getMessage.contains("""400 Bad Request {"errorCode":"RESOURCE_DOES_NOT_EXIST""""))
  }

  integrationTest("azure support") {
    for (azureTableName <- "table_wasb" :: "table_abfs" :: Nil) {
      val tablePath = testProfileFile.getCanonicalPath + s"#share_azure.default.${azureTableName}"
      checkAnswer(
        spark.read.format("deltaSharing").load(tablePath),
        Row("foo bar", "foo bar") :: Nil
      )
    }
  }

  integrationTest("gcp support") {
    val tablePath = testProfileFile.getCanonicalPath + s"#share_gcp.default.table_gcs"
    checkAnswer(
      spark.read.format("deltaSharing").load(tablePath),
      Row("foo bar", "foo bar") :: Nil
    )
  }

  integrationTest("random access stream") {
    // Set maxConnections to 1 so that if we leak any connection, we will hang forever because any
    // further request won't be able to get a free connection from the pool.
    withSQLConf("spark.delta.sharing.network.maxConnections" -> "1") {
      val seed = System.currentTimeMillis()
      // scalastyle:off println
      println(s"seed for random access stream test: $seed")
      // scalastyle:on println
      val r = new Random(seed)
      val tablePath = testProfileFile.getCanonicalPath + "#share1.default.table1"
      val file = spark.read.format("deltaSharing").load(tablePath).inputFiles.head
      var content: Array[Byte] = null
      withSQLConf("spark.delta.sharing.loadDataFilesInMemory" -> "true") {
        FileSystem.closeAll()
        val fs = new Path(file).getFileSystem(spark.sessionState.newHadoopConf())
        val input = fs.open(new Path(file))
        assert(input.getWrappedStream.isInstanceOf[InMemoryHttpInputStream])
        try {
          content = IOUtils.toByteArray(input)
        } finally {
          input.close()
        }
      }
      FileSystem.closeAll()
      val fs = new Path(file).getFileSystem(spark.sessionState.newHadoopConf())
      val input = fs.open(new Path(file))
      try {
        assert(input.getWrappedStream.isInstanceOf[RandomAccessHttpInputStream])
        intercept[EOFException] {
          input.seek(-1)
        }
        intercept[EOFException] {
          input.seek(content.length)
        }
        var currentPos = 0
        var i = r.nextInt(10) + 5
        while (i >= 0) {
          i -= 1
          val nextAction = r.nextInt(2)
          if (nextAction == 0) { // seek
            currentPos = r.nextInt(content.length)
            input.seek(currentPos)
          } else { // read
            val readSize = r.nextInt(content.length - currentPos + 1)
            val buf = new Array[Byte](readSize)
            input.readFully(buf)
            assert(buf.toList == content.slice(currentPos, currentPos + readSize).toList)
            currentPos += readSize
          }
        }
      } finally {
        input.close()
      }
    }
  }

  test("creating a managed table should fail") {
    val e = intercept[IllegalArgumentException] {
      sql("CREATE table foo USING deltaSharing")
    }
    assert(e.getMessage.contains("LOCATION must be specified"))
  }

  import java.time.LocalDateTime
  import org.apache.spark.sql.execution.streaming.StreamingQueryWrapper
  import org.apache.spark.sql.streaming.StreamingQuery
  import org.apache.spark.sql.streaming.OutputMode
  def printQuery(query: StreamingQuery): Unit = {
//    Console.println(s"--------[linzhou]--------[query][${query}]")
//    Console.println(s"--------[linzhou]--------[query.id][${query.id}]")
//    Console.println(s"--------[linzhou]--------[query.runId][${query.runId}]")
//    Console.println(s"--------[linzhou]--------[query.name][${query.name}]")
//    Console.println(s"--------[linzhou]--------[query.explain][${query.explain}]")
    Console.println(s"--------[linzhou]--------[query.exception][${query.exception}]")
    Console.println(s"--------[linzhou]--------[query.recentProgress][${query.recentProgress}]")
    Console.println(s"--------[linzhou]--------[query.lastProgress][${query.lastProgress}]")
  }

  integrationTest("stream query test - exceptions") {
    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_cdf_enabled"
    var query = spark.readStream.format("deltaSharing").option("path", tablePath)
      .option("startingVersion", "0")
      .load().writeStream.format("console").start()
    var errorMessage = intercept[StreamingQueryException] {
      query.awaitTermination()   // block until query is terminated, with stop() or with error
    }.getMessage
    assert(errorMessage.contains("Detected deleted data from streaming source at version 2"))

    query = spark.readStream.format("deltaSharing").option("path", tablePath)
      .option("startingVersion", "0")
      .option("ignoreDeletes", "true")
      .load().writeStream.format("console").start()
    errorMessage = intercept[StreamingQueryException] {
      query.awaitTermination()   // block until query is terminated, with stop() or with error
    }.getMessage
    assert(errorMessage.contains("Detected a data update in the source table at version 3"))

    errorMessage = intercept[UnsupportedOperationException] {
      query = spark.readStream.format("deltaSharing").option("path", tablePath)
        .option("startingVersion", "0")
        .option("readChangeFeed", "true")
        .load().writeStream.format("console").start()
    }.getMessage
    assert(errorMessage.contains("CDF is not supported in Delta Sharing Streaming yet"))

    errorMessage = intercept[UnsupportedOperationException] {
      query = spark.readStream.format("deltaSharing").option("path", tablePath)
        .option("startingVersion", "0")
        .option("readChangeData", "true")
        .load().writeStream.format("console").start()
    }.getMessage
    assert(errorMessage.contains("CDF is not supported in Delta Sharing Streaming yet"))
  }

  integrationTest("stream query test - test") {
    Console.println(s"--------[linzhou]-----------[test-start][${LocalDateTime.now()}]")

    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.cdf_table_cdf_enabled"
    val query = spark.readStream.format("deltaSharing").option("path", tablePath)
      .option("startingVersion", "0")
      .option("ignoreDeletes", "true")
      .option("ignoreChanges", "true")
      .load().writeStream
      .queryName("ds_stream_test")
      .outputMode(OutputMode.Update)
      .format("memory").start()

    val error = intercept[StreamingQueryException] {
      query.awaitTermination()   // block until query is terminated, with stop() or with error
    }
    Console.println(s"--------[linzhou]----[error][${error.printStackTrace}]")
//    var i = 0
//    while (i < 10 && !spark.streams.active.isEmpty) {
//      Console.println(s"--------[linzhou]-----------[test-i][${i}]")
//      printQuery(query)
//      i += 1
//      Thread.sleep(10000)
//    }
//    Console.println(s"--------[linzhou]-----------[test-i][${i}]")
//    printQuery(query)
//    query.stop()
//    Console.println(s"--------[linzhou]-----------[test-end][${LocalDateTime.now()}]")
  }
}
