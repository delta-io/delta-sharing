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

import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{QueryTest, Row}
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
}
