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

package io.delta.sharing.spark.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.SQLConf

class ConfUtilsSuite extends SparkFunSuite {
  import ConfUtils._

  private def newConf(properties: Map[String, String] = Map.empty): Configuration = {
    val conf = new Configuration()
    properties.foreach(p => conf.setStrings(p._1, p._2))
    conf
  }

  private def newSqlConf(properties: Map[String, String] = Map.empty): SQLConf = {
    val sqlConf = new SQLConf()
    properties.foreach(p => sqlConf.setConfString(p._1, p._2))
    sqlConf
  }

  test("numRetries") {
    assert(numRetries(newConf()) == NUM_RETRIES_DEFAULT)
    assert(numRetries(newConf(Map(NUM_RETRIES_CONF -> "100"))) == 100)
    intercept[IllegalArgumentException] {
      numRetries(newConf(Map(NUM_RETRIES_CONF -> "-1")))
    }.getMessage.contains(NUM_RETRIES_CONF)

    assert(numRetries(newSqlConf()) == NUM_RETRIES_DEFAULT)
    assert(numRetries(newSqlConf(Map(NUM_RETRIES_CONF -> "50"))) == 50)
    intercept[IllegalArgumentException] {
      numRetries(newSqlConf(Map(NUM_RETRIES_CONF -> "-1")))
    }.getMessage.contains(NUM_RETRIES_CONF)
  }

  test("maxRetryDuration") {
    assert(maxRetryDurationMillis(newConf()) == MAX_RETRY_DURATION_DEFAULT_MILLIS)
    assert(maxRetryDurationMillis(newConf(Map(MAX_RETRY_DURATION_CONF -> "50000"))) == 50000L)
    intercept[IllegalArgumentException] {
      maxRetryDurationMillis(newConf(Map(MAX_RETRY_DURATION_CONF -> "-1")))
    }.getMessage.contains(MAX_RETRY_DURATION_CONF)

    assert(maxRetryDurationMillis(newSqlConf()) == MAX_RETRY_DURATION_DEFAULT_MILLIS)
    assert(maxRetryDurationMillis(newSqlConf(Map(MAX_RETRY_DURATION_CONF -> "25000"))) == 25000L)
    intercept[IllegalArgumentException] {
      maxRetryDurationMillis(newSqlConf(Map(MAX_RETRY_DURATION_CONF -> "-1")))
    }.getMessage.contains(MAX_RETRY_DURATION_CONF)
  }

  test("timeout") {
    assert(timeoutInSeconds(newConf()) == 320)
    assert(timeoutInSeconds(newConf(Map(TIMEOUT_CONF -> "100s"))) == 100)
    intercept[IllegalArgumentException] {
      timeoutInSeconds(newConf(Map(TIMEOUT_CONF -> "-1")))
    }.getMessage.contains(TIMEOUT_CONF)
    intercept[IllegalArgumentException] {
      timeoutInSeconds(newConf(Map(TIMEOUT_CONF -> "9999999999")))
    }.getMessage.contains(TIMEOUT_CONF)

    assert(timeoutInSeconds(newSqlConf()) == 320)
    assert(timeoutInSeconds(newSqlConf(Map(TIMEOUT_CONF -> "50s"))) == 50)
    intercept[IllegalArgumentException] {
      timeoutInSeconds(newSqlConf(Map(TIMEOUT_CONF -> "-1")))
    }.getMessage.contains(TIMEOUT_CONF)
    intercept[IllegalArgumentException] {
      timeoutInSeconds(newSqlConf(Map(TIMEOUT_CONF -> "9999999999")))
    }.getMessage.contains(TIMEOUT_CONF)
  }

  test("maxConnections") {
    assert(maxConnections(newConf()) == MAX_CONNECTION_DEFAULT)
    assert(maxConnections(newConf(Map(MAX_CONNECTION_CONF -> "100"))) == 100)
    intercept[IllegalArgumentException] {
      maxConnections(newConf(Map(MAX_CONNECTION_CONF -> "-1")))
    }.getMessage.contains(MAX_CONNECTION_CONF)
  }
}
