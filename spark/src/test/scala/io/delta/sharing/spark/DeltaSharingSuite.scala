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
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType, TimestampType}

import io.delta.sharing.client.{InMemoryHttpInputStream, RandomAccessHttpInputStream}
import io.delta.sharing.spark.TestUtils._

class DeltaSharingSuite extends QueryTest with SharedSparkSession with DeltaSharingIntegrationTest {

  import testImplicits._

  integrationTest("table1 passing profile with read options") {
    val tablePath = "share1.default.table1"
    val expected = Seq(
      Row(sqlTimestamp("2021-04-27 23:32:02.07"), sqlDate("2021-04-28")),
      Row(sqlTimestamp("2021-04-27 23:32:22.421"), sqlDate("2021-04-28"))
    )
    val readOptions = Map(
      "endpoint" -> s"https://localhost:$TEST_PORT/delta-sharing",
      "bearerToken" -> "dapi5e3574ec767ca1548ae5bbed1a2dc04d",
      "shareCredentialsVersion" -> "1"
    )
    // Test DataFrame API with inline credentials
    checkAnswer(spark.read.format("deltaSharing").options(readOptions).load(tablePath), expected)
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

  integrationTest("cdf_table_with_partition: filter success") {
    val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_with_partition"

    val expected = Seq(
      Row("1", 1, sqlDate("2020-01-01")),
      Row("2", 2, sqlDate("2020-02-02"))
    )
    val result = spark.read.format("deltaSharing").load(tablePath)
    checkAnswer(result, expected)

    // should work when filtering on partition columns
    val filtered = spark.read.format("deltaSharing")
      .load(tablePath)
      .filter($"birthday" === "2020-01-01")

    checkAnswer(
      filtered,
      Seq(
        Row("1", 1, sqlDate("2020-01-01"))
      )
    )

    // should work when filtering on non-partition columns
    val filtered2 = spark.read.format("deltaSharing")
      .load(tablePath)
      .filter($"age" === "2")
      .select("name", "birthday")

    checkAnswer(
      filtered2,
      Seq(
        Row("2", sqlDate("2020-02-02"))
      )
    )
  }

  integrationTest("cdf_table_cdf_enabled query without version") {
    val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_cdf_enabled"
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
    val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_cdf_enabled"
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
    val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_cdf_enabled"
    val expected = Seq()
    val errorMessage = intercept[IllegalArgumentException] {
      checkAnswer(
        spark.read.format("deltaSharing").option("versionAsOf", "3x").load(tablePath), expected)
    }.getMessage
    assert(errorMessage.contains("Invalid value '3x' for option 'versionAsOf'"))
  }

  integrationTest("cdf_table_cdf_enabled timestamp exception") {
    val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_cdf_enabled"
    val expected = Seq()
    var errorMessage = intercept[io.delta.sharing.client.util.UnexpectedHttpStatus] {
      checkAnswer(
        spark.read
          .format("deltaSharing")
          .option("timestampAsOf", "2000-01-01 00:00:00")
          .load(tablePath),
        expected
      )
    }.getMessage
    assert(errorMessage.contains("The provided timestamp "))
    assert(errorMessage.contains("The provided timestamp "))

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
    val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_cdf_enabled"

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

  integrationTest("table_changes: cdf_table_with_partition") {
    val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_with_partition"

    val expected = Seq(
      Row("1", 1, sqlDate("2020-01-01"), 1L, 1651614980000L, "insert"),
      Row("2", 2, sqlDate("2020-01-01"), 1L, 1651614980000L, "insert"),
      Row("3", 3, sqlDate("2020-03-03"), 1L, 1651614980000L, "insert"),
      Row("2", 2, sqlDate("2020-01-01"), 2L, 1651614986000L, "update_preimage"),
      Row("2", 2, sqlDate("2020-02-02"), 2L, 1651614986000L, "update_postimage"),
      Row("3", 3, sqlDate("2020-03-03"), 3L, 1651614994000L, "delete")
    )
    val result = spark.read.format("deltaSharing")
      .option("readChangeFeed", "true")
      .option("startingVersion", 1)
      .option("endingVersion", 3).load(tablePath)
    checkAnswer(result, expected)

    // should work when filtering on partition columns
    val filtered = spark.read.format("deltaSharing")
      .option("readChangeFeed", "true")
      .option("startingVersion", 1)
      .option("endingVersion", 3)
      .load(tablePath)
      .filter($"birthday" === "2020-01-01")

    checkAnswer(
      filtered,
      Seq(
        Row("1", 1, sqlDate("2020-01-01"), 1L, 1651614980000L, "insert"),
        Row("2", 2, sqlDate("2020-01-01"), 1L, 1651614980000L, "insert"),
        Row("2", 2, sqlDate("2020-01-01"), 2L, 1651614986000L, "update_preimage")
      )
    )

    // should work when filtering on non-partition columns
    val filtered2 = spark.read.format("deltaSharing")
      .option("readChangeFeed", "true")
      .option("startingVersion", 1)
      .option("endingVersion", 3)
      .load(tablePath)
      .filter($"age" === "2")
      .select("name", "birthday", "_commit_version", "_commit_timestamp", "_change_type")

    checkAnswer(
      filtered2,
      Seq(
        Row("2", sqlDate("2020-01-01"), 2L, 1651614986000L, "update_preimage"),
        Row("2", sqlDate("2020-01-01"), 1L, 1651614980000L, "insert"),
        Row("2", sqlDate("2020-02-02"), 2L, 1651614986000L, "update_postimage")
      )
    )

    // should work when filtering on two columns
    val filtered3 = spark.read.format("deltaSharing")
      .option("readChangeFeed", "true")
      .option("startingVersion", 1)
      .option("endingVersion", 3)
      .load(tablePath)
      .filter($"age" === "2" && $"birthday" === "2020-01-01")
      .select("name", "birthday", "_commit_version", "_commit_timestamp", "_change_type")

    checkAnswer(
      filtered3,
      Seq(
        Row("2", sqlDate("2020-01-01"), 2L, 1651614986000L, "update_preimage"),
        Row("2", sqlDate("2020-01-01"), 1L, 1651614980000L, "insert")
      )
    )

    // should work when filtering on added cdf columns
    val filtered4 = spark.read.format("deltaSharing")
      .option("readChangeFeed", "true")
      .option("startingVersion", 1)
      .option("endingVersion", 3)
      .load(tablePath)
      .filter($"_commit_version" === "2")

    checkAnswer(
      filtered4,
      Seq(
        Row("2", 2, sqlDate("2020-01-01"), 2L, 1651614986000L, "update_preimage"),
        Row("2", 2, sqlDate("2020-02-02"), 2L, 1651614986000L, "update_postimage")
      )
    )

    val filtered5 = spark.read.format("deltaSharing")
      .option("readChangeFeed", "true")
      .option("startingVersion", 1)
      .option("endingVersion", 3)
      .load(tablePath)
      .filter(col("_change_type").like("%nser%"))

    checkAnswer(
      filtered5,
      Seq(
        Row("1", 1, sqlDate("2020-01-01"), 1L, 1651614980000L, "insert"),
        Row("2", 2, sqlDate("2020-01-01"), 1L, 1651614980000L, "insert"),
        Row("3", 3, sqlDate("2020-03-03"), 1L, 1651614980000L, "insert")
      )
    )

    val filtered6 = spark.read.format("deltaSharing")
      .option("readChangeFeed", "true")
      .option("startingVersion", 1)
      .option("endingVersion", 3)
      .load(tablePath)
      .filter(col("_change_type").contains("update"))

    checkAnswer(
      filtered6,
      Seq(
        Row("2", 2, sqlDate("2020-01-01"), 2L, 1651614986000L, "update_preimage"),
        Row("2", 2, sqlDate("2020-02-02"), 2L, 1651614986000L, "update_postimage")
      )
    )
  }

  integrationTest("table_changes_empty: cdf_table_cdf_enabled") {
    val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_cdf_enabled"

    val result = spark.read.format("deltaSharing")
      .option("readChangeFeed", "true")
      .option("startingVersion", 5).load(tablePath)
    checkAnswer(result, Seq.empty)
  }

  integrationTest("table_changes_with_timestamp: cdf_table_cdf_enabled") {
    val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_cdf_enabled"

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
    val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_with_vacuum"

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
    val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_with_vacuum"

    // parquet file is vacuumed, will see 404 error when requsting the presigned url
    val ex = intercept[org.apache.spark.SparkException] {
      val df = spark.read.format("deltaSharing")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0).load(tablePath)
      checkAnswer(df, Nil)
    }
    assert (ex.getCause.getMessage.contains("404 Not Found"))
    assert (ex.getCause.getMessage.contains("c000.snappy.parquet"))
  }

  integrationTest("table_changes_exception: cdf_table_missing_log") {
    val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_missing_log"

    // log file is missing
    val ex = intercept[io.delta.sharing.client.util.UnexpectedHttpStatus] {
      val df = spark.read.format("deltaSharing")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0).load(tablePath)
      checkAnswer(df, Nil)
    }
    assert (ex.getMessage.contains("""400 Bad Request"""))
    assert (ex.getMessage.contains("""{"errorCode":"RESOURCE_DOES_NOT_EXIST""""))
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

  integrationTest("async query") {
    withSQLConf("spark.delta.sharing.network.useAsyncQuery" -> "true") {
      for (azureTableName <- "table_wasb" :: "table_abfs" :: Nil) {
        val tablePath = testProfileFile.getCanonicalPath + s"#share_azure.default.${azureTableName}"
        checkAnswer(
          spark.read.format("deltaSharing").load(tablePath),
          Row("foo bar", "foo bar") :: Nil
        )
      }
    }
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

  integrationTest("table1 with storage proxy") {
    val proxyServer = new TestStorageProxyServer
    proxyServer.initialize()
    withSQLConf("spark.delta.sharing.network.proxyHost" -> s"${proxyServer.getHost()}",
      "spark.delta.sharing.network.proxyPort" -> s"${proxyServer.getPort()}",
      "spark.delta.sharing.network.never.use.https" -> "true") {

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
      proxyServer.stop()
    }
  }

  integrationTest("spark read limit") {
    val spark = SparkSession.active
    spark.sessionState.conf.setConfString("spark.delta.sharing.client.class",
      classOf[TestDeltaSharingClient].getName)
    spark.sessionState.conf.setConfString("spark.delta.sharing.profile.provider.class",
      "io.delta.sharing.client.DeltaSharingFileProfileProvider")
    TestDeltaSharingClient.clear

    val tablePath = testProfileFile.getCanonicalPath + "#share1.default.table1"
    spark.read
      .format("deltaSharing")
      .load(tablePath)
      .collect()
    assert(TestDeltaSharingClient.limits === Seq.empty)

    spark.read
      .format("deltaSharing")
      .load(tablePath)
      .limit(1)
      .collect()
    assert(TestDeltaSharingClient.limits === Seq(1L))
  }

  integrationTest("async query timeout") {
    // Test that async query times out when it exceeds the specified timeout
    withSQLConf(
      "spark.delta.sharing.network.useAsyncQuery" -> "true",
      "spark.delta.sharing.network.asyncQueryTimeout" -> "1", // 1ms timeout
      "spark.delta.sharing.network.asyncQueryPollIntervalMillis" -> "100"
    ) {
      val tablePath = testProfileFile.getCanonicalPath + "#share_azure.default.table_wasb"

      // Note: This test requires server-side support for async queries that take longer
      // than timeout In a real scenario, this would timeout if the server keeps returning
      // queryStatus
      val ex = intercept[IllegalStateException] {
        spark.read.format("deltaSharing").load(tablePath).collect()
      }
      assert(ex.getMessage.contains("Query is timed out after") ||
             ex.getMessage.contains("1000 ms"))
    }
  }

  integrationTest("async query initial query table errors") {
    // Test error conditions in the initial query table request (DeltaSharingClient.scala:L650)
    withSQLConf("spark.delta.sharing.network.useAsyncQuery" -> "true") {
      val tablePath = testProfileFile.getCanonicalPath + "#share1.default.nonexistent_table"

      // Test 1: Invalid table should throw UnexpectedHttpStatus
      val ex1 = intercept[io.delta.sharing.client.util.UnexpectedHttpStatus] {
        spark.read.format("deltaSharing").load(tablePath).collect()
      }
      assert(ex1.getMessage.contains("404") || ex1.getMessage.contains("Not Found"))
    }
  }

  integrationTest("async query polling errors") {
    // Test error conditions during polling (DeltaSharingClient.scala:L1099)
    withSQLConf(
      "spark.delta.sharing.network.useAsyncQuery" -> "true",
      "spark.delta.sharing.network.asyncQueryPollIntervalMillis" -> "100"
    ) {
      // Test 1: Inconsistent queryId in polling response
      // Server will change the queryId mid-query for tables ending with "_change_query_id"
      val tablePath1 = testProfileFile.getCanonicalPath +
        "#share_azure.default.table_wasb_change_query_id"
      val ex1 = intercept[IllegalStateException] {
        spark.read.format("deltaSharing").load(tablePath1).collect()
      }
      assert(ex1.getMessage.contains("QueryId is not consistent") ||
             ex1.getMessage.contains("query"))

      // Test 2: Missing queryId when query is still pending
      // Server will return null queryId for tables ending with "_change_query_id_to_be_null"
      val tablePath2 = testProfileFile.getCanonicalPath +
        "#share_azure.default.table_wasb_change_query_id_to_be_null"
      val ex2 = intercept[IllegalStateException] {
        spark.read.format("deltaSharing").load(tablePath2).collect()
      }
      assert(ex2.getMessage.contains("QueryId is not returned") ||
             ex2.getMessage.contains("null"))
    }
  }

  integrationTest("async query handles load table failures") {
    withSQLConf(
      "spark.delta.sharing.network.useAsyncQuery" -> "true",
      "spark.delta.sharing.network.asyncQueryPollIntervalMillis" -> "100"
    ) {
      // Test: Server encounters error loading table after polling
      // Server will try to load a non-existent table on the 2nd poll for tables ending with
      // "_bad_table", which will cause loadTable to fail
      val tablePath3 = testProfileFile.getCanonicalPath +
        "#share_azure.default.table_wasb_bad_table"
      val ex3 = intercept[io.delta.sharing.client.util.UnexpectedHttpStatus] {
        spark.read.format("deltaSharing").load(tablePath3).collect()
      }
      assert(ex3.getMessage.contains("404") || ex3.getMessage.contains("400") ||
             ex3.getMessage.contains("does not exist"))
    }
  }

  integrationTest("async query error handling in initial response") {
    // Test various error conditions that can occur in getNDJsonWithAsync
    withSQLConf("spark.delta.sharing.network.useAsyncQuery" -> "true") {
      // Test 1: Invalid version parameter
      val tablePath = testProfileFile.getCanonicalPath + "#share8.default.cdf_table_cdf_enabled"
      val ex1 = intercept[io.delta.sharing.client.util.UnexpectedHttpStatus] {
        spark.read
          .format("deltaSharing")
          .option("versionAsOf", "999999") // Non-existent version
          .load(tablePath)
          .collect()
      }
      assert(ex1.getMessage.contains("400") || ex1.getMessage.contains("404"))

      // Test 2: Invalid timestamp parameter
      val ex2 = intercept[io.delta.sharing.client.util.UnexpectedHttpStatus] {
        spark.read
          .format("deltaSharing")
          .option("timestampAsOf", "1970-01-01 00:00:00")
          .load(tablePath)
          .collect()
      }
      assert(ex2.getMessage.contains("400") ||
             ex2.getMessage.contains("The provided timestamp"))
    }
  }
}

class DeltaSharingWithParquetIOCacheEnabledSuite extends DeltaSharingSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.delta.sharing.client.sparkParquetIOCache.enabled", "true")
  }
}
