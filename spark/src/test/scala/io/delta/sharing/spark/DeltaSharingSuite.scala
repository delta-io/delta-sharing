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
}
