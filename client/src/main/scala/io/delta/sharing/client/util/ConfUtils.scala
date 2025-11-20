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

package io.delta.sharing.client.util

import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.internal.SQLConf

object ConfUtils {

  val NUM_RETRIES_CONF = "spark.delta.sharing.network.numRetries"
  val NUM_RETRIES_DEFAULT = 3

  val MAX_RETRY_DURATION_CONF = "spark.delta.sharing.network.maxRetryDuration"
  val MAX_RETRY_DURATION_DEFAULT_MILLIS = 10L * 60L* 1000L /* 10 mins */

  val RETRY_SLEEP_INTERVAL_CONF = "spark.delta.sharing.network.retrySleepInterval"
  val RETRY_SLEEP_INTERVAL_DEFAULT_MILLIS = 1000L /* 1 second */

  val ASYNC_QUERY_POLL_INTERVAL_CONF = "spark.delta.sharing.network.asyncQueryRetryInterval"
  val ASYNC_QUERY_POLL_INTERVAL_DEFAULT_MILLIS = 10L * 1000L /* 10 seconds */

  val ASYNC_QUERY_TIMEOUT_CONF = "spark.delta.sharing.network.asyncQueryTimeout"
  val ASYNC_QUERY_TIMEOUT_DEFAULT_MILLIS = 10L * 60L * 1000L /* 10 mins */

  val USE_ASYNC_QUERY_CONF = "spark.delta.sharing.network.useAsyncQuery"
  val USE_ASYNC_QUERY_DEFAULT = "false"

  val INCLUDE_END_STREAM_ACTION_CONF = "spark.delta.sharing.query.includeEndStreamAction"
  val INCLUDE_END_STREAM_ACTION_DEFAULT = "false"

  val TIMEOUT_CONF = "spark.delta.sharing.network.timeout"
  val TIMEOUT_DEFAULT = "320s"

  // Note: There is a separate pool for proxy and non-proxy connections.
  //       Thus, if you use both spark.delta.sharing.network.proxyHost and
  //       spark.delta.sharing.network.noProxyHosts,
  //       the max number of connections will be 2 * spark.delta.sharing.network.maxConnections.
  val MAX_CONNECTION_CONF = "spark.delta.sharing.network.maxConnections"
  val MAX_CONNECTION_DEFAULT = 64

  val SSL_TRUST_ALL_CONF = "spark.delta.sharing.network.sslTrustAll"
  val SSL_TRUST_ALL_DEFAULT = "false"

  val PROFILE_PROVIDER_CLASS_CONF = "spark.delta.sharing.profile.provider.class"
  val PROFILE_PROVIDER_CLASS_DEFAULT = "io.delta.sharing.client.DeltaSharingFileProfileProvider"

  val CLIENT_CLASS_CONF = "spark.delta.sharing.client.class"
  val CLIENT_CLASS_DEFAULT = "io.delta.sharing.client.DeltaSharingRestClient"

  val JSON_PREDICATE_CONF = "spark.delta.sharing.jsonPredicateHints.enabled"
  val JSON_PREDICATE_DEFAULT = "true"

  val JSON_PREDICATE_V2_CONF = "spark.delta.sharing.jsonPredicateV2Hints.enabled"
  val JSON_PREDICATE_V2_DEFAULT = "true"

  val QUERY_PAGINATION_ENABLED_CONF = "spark.delta.sharing.queryPagination.enabled"
  val QUERY_PAGINATION_ENABLED_DEFAULT = "false"

  val MAX_FILES_CONF = "spark.delta.sharing.maxFilesPerQueryRequest"
  val MAX_FILES_DEFAULT = 100000

  val QUERY_TABLE_VERSION_INTERVAL_SECONDS =
    "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds"
  val QUERY_TABLE_VERSION_INTERVAL_SECONDS_DEFAULT = "30s"
  val MINIMUM_TABLE_VERSION_INTERVAL_SECONDS = 10

  val LIMIT_PUSHDOWN_ENABLED_CONF = "spark.delta.sharing.limitPushdown.enabled"
  val LIMIT_PUSHDOWN_ENABLED_DEFAULT = "true"

  val PROXY_HOST = "spark.delta.sharing.network.proxyHost"
  val PROXY_PORT = "spark.delta.sharing.network.proxyPort"
  val NO_PROXY_HOSTS = "spark.delta.sharing.network.noProxyHosts"

  val OAUTH_RETRIES_CONF = "spark.delta.sharing.oauth.tokenExchangeMaxRetries"
  val OAUTH_RETRIES_DEFAULT = 5

  val OAUTH_MAX_RETRY_DURATION_CONF =
    "spark.delta.sharing.oauth.tokenExchangeMaxRetryDurationInSeconds"
  val OAUTH_MAX_RETRY_DURATION_SECONDS_DEFAULT = 1 * 60 /* 1 mins */

  val OAUTH_EXPIRATION_THRESHOLD_CONF =
    "spark.delta.sharing.oauth.tokenRenewalThresholdInSeconds"
  val OAUTH_EXPIRATION_THRESHOLD_SECONDS_DEFAULT = 10 * 60 /* 10 mins */

  val NEVER_USE_HTTPS = "spark.delta.sharing.network.never.use.https"
  val NEVER_USE_HTTPS_DEFAULT = "false"

  val STRUCTURAL_SCHEMA_MATCH_CONF = "spark.delta.sharing.client.useStructuralSchemaMatch"
  val STRUCTURAL_SCHEMA_MATCH_DEFAULT = "false"

  val SPARK_PARQUET_IO_CACHE_CONF = "spark.delta.sharing.client.sparkParquetIOCache.enabled"
  val SPARK_PARQUET_IO_CACHE_DEFAULT = "false"

  val LOG_PRESIGNED_URL_ACCESS_CONF = "spark.delta.sharing.client.logPreSignedUrlAccess.enabled"
  val LOG_PRESIGNED_URL_ACCESS_DEFAULT = false

  val VERSION_MISMATCH_CHECK_ENABLED_CONF = "spark.delta.sharing.client.versionMismatchCheckEnabled"
  val VERSION_MISMATCH_CHECK_ENABLED_DEFAULT = false

  def getProxyConfig(conf: Configuration): Option[ProxyConfig] = {
    val proxyHost = conf.get(PROXY_HOST, null)
    val proxyPortAsString = conf.get(PROXY_PORT, null)

    if (proxyHost == null && proxyPortAsString == null) {
      return None
    }

    validateNonEmpty(proxyHost, PROXY_HOST)
    validateNonEmpty(proxyPortAsString, PROXY_PORT)
    val proxyPort = proxyPortAsString.toInt
    validatePortNumber(proxyPort, PROXY_PORT)

    val noProxyList = conf.getTrimmedStrings(NO_PROXY_HOSTS).toSeq
    Some(ProxyConfig(proxyHost, proxyPort, noProxyHosts = noProxyList))
  }

  def getNeverUseHttps(conf: Configuration): Boolean = {
    conf.getBoolean(NEVER_USE_HTTPS, NEVER_USE_HTTPS_DEFAULT.toBoolean)
  }

  def numRetries(conf: Configuration): Int = {
    val numRetries = conf.getInt(NUM_RETRIES_CONF, NUM_RETRIES_DEFAULT)
    validateNonNeg(numRetries, NUM_RETRIES_CONF)
    numRetries
  }

  def numRetries(conf: SQLConf): Int = {
    val numRetries = conf.getConfString(NUM_RETRIES_CONF, NUM_RETRIES_DEFAULT.toString).toInt
    validateNonNeg(numRetries, NUM_RETRIES_CONF)
    numRetries
  }

  def maxRetryDurationMillis(conf: Configuration): Long = {
    val maxDur = conf.getLong(MAX_RETRY_DURATION_CONF, MAX_RETRY_DURATION_DEFAULT_MILLIS)
    validateNonNeg(maxDur, MAX_RETRY_DURATION_CONF)
    maxDur
  }

  def maxRetryDurationMillis(conf: SQLConf): Long = {
    val maxDur =
      conf.getConfString(MAX_RETRY_DURATION_CONF, MAX_RETRY_DURATION_DEFAULT_MILLIS.toString).toLong
    validateNonNeg(maxDur, MAX_RETRY_DURATION_CONF)
    maxDur
  }

  def retrySleepIntervalMillis(conf: Configuration): Long = {
    val interval = conf.getLong(RETRY_SLEEP_INTERVAL_CONF, RETRY_SLEEP_INTERVAL_DEFAULT_MILLIS)
    validateNonNeg(interval, RETRY_SLEEP_INTERVAL_CONF)
    interval
  }

  def retrySleepIntervalMillis(conf: SQLConf): Long = {
    val interval =
      conf.getConfString(
        RETRY_SLEEP_INTERVAL_CONF,
        RETRY_SLEEP_INTERVAL_DEFAULT_MILLIS.toString).toLong
    validateNonNeg(interval, RETRY_SLEEP_INTERVAL_CONF)
    interval
  }

  def asyncQueryPollIntervalMillis(conf: Configuration): Long = {
    val interval = conf.getLong(
      ASYNC_QUERY_POLL_INTERVAL_CONF,
      ASYNC_QUERY_POLL_INTERVAL_DEFAULT_MILLIS)
    validateNonNeg(interval, ASYNC_QUERY_POLL_INTERVAL_CONF)
    interval
  }

  def asyncQueryPollIntervalMillis(conf: SQLConf): Long = {
    val interval = conf.getConfString(
      ASYNC_QUERY_POLL_INTERVAL_CONF,
      ASYNC_QUERY_POLL_INTERVAL_DEFAULT_MILLIS.toString).toLong
    validateNonNeg(interval, ASYNC_QUERY_POLL_INTERVAL_CONF)
    interval
  }

  def asyncQueryTimeout(conf: Configuration): Long = {
    val timeout = conf.getLong(ASYNC_QUERY_TIMEOUT_CONF, ASYNC_QUERY_TIMEOUT_DEFAULT_MILLIS)
    validateNonNeg(timeout, ASYNC_QUERY_TIMEOUT_CONF)
    timeout
  }

  def asyncQueryTimeout(conf: SQLConf): Long = {
    val timeout = conf.getConfString(
      ASYNC_QUERY_TIMEOUT_CONF,
      ASYNC_QUERY_TIMEOUT_DEFAULT_MILLIS.toString).toLong
    validateNonNeg(timeout, ASYNC_QUERY_TIMEOUT_CONF)
    timeout
  }

  def useAsyncQuery(conf: Configuration): Boolean = {
    conf.getBoolean(USE_ASYNC_QUERY_CONF, USE_ASYNC_QUERY_DEFAULT.toBoolean)
  }

  def useAsyncQuery(conf: SQLConf): Boolean = {
    conf.getConfString(USE_ASYNC_QUERY_CONF, USE_ASYNC_QUERY_DEFAULT).toBoolean
  }

  def includeEndStreamAction(conf: Configuration): Boolean = {
    conf.getBoolean(INCLUDE_END_STREAM_ACTION_CONF, INCLUDE_END_STREAM_ACTION_DEFAULT.toBoolean)
  }

  def includeEndStreamAction(conf: SQLConf): Boolean = {
    conf.getConfString(INCLUDE_END_STREAM_ACTION_CONF, INCLUDE_END_STREAM_ACTION_DEFAULT).toBoolean
  }

  def timeoutInSeconds(conf: Configuration): Int = {
    val timeoutStr = conf.get(TIMEOUT_CONF, TIMEOUT_DEFAULT)
    toTimeInSeconds(timeoutStr, TIMEOUT_CONF)
  }

  def timeoutInSeconds(conf: SQLConf): Int = {
    val timeoutStr = conf.getConfString(TIMEOUT_CONF, TIMEOUT_DEFAULT)
    toTimeInSeconds(timeoutStr, TIMEOUT_CONF)
  }

  def maxConnections(conf: Configuration): Int = {
    val maxConn = conf.getInt(MAX_CONNECTION_CONF, MAX_CONNECTION_DEFAULT)
    validateNonNeg(maxConn, MAX_CONNECTION_CONF)
    maxConn
  }

  def sslTrustAll(conf: SQLConf): Boolean = {
    conf.getConfString(SSL_TRUST_ALL_CONF, SSL_TRUST_ALL_DEFAULT).toBoolean
  }

  def profileProviderClass(conf: SQLConf): String = {
    conf.getConfString(PROFILE_PROVIDER_CLASS_CONF, PROFILE_PROVIDER_CLASS_DEFAULT)
  }

  def clientClass(conf: SQLConf): String = {
    conf.getConfString(CLIENT_CLASS_CONF, CLIENT_CLASS_DEFAULT)
  }

  def jsonPredicatesEnabled(conf: SQLConf): Boolean = {
    conf.getConfString(JSON_PREDICATE_CONF, JSON_PREDICATE_DEFAULT).toBoolean
  }

  def jsonPredicatesV2Enabled(conf: SQLConf): Boolean = {
    conf.getConfString(JSON_PREDICATE_V2_CONF, JSON_PREDICATE_V2_DEFAULT).toBoolean
  }

  def queryTablePaginationEnabled(conf: SQLConf): Boolean = {
    conf.getConfString(QUERY_PAGINATION_ENABLED_CONF, QUERY_PAGINATION_ENABLED_DEFAULT).toBoolean
  }

  def maxFilesPerQueryRequest(conf: SQLConf): Int = {
    val maxFiles = conf.getConfString(MAX_FILES_CONF, MAX_FILES_DEFAULT.toString).toInt
    validatePositive(maxFiles, MAX_FILES_CONF)
    maxFiles
  }

  def streamingQueryTableVersionIntervalSeconds(conf: SQLConf): Int = {
    val intervalStr = conf.getConfString(
      QUERY_TABLE_VERSION_INTERVAL_SECONDS,
      QUERY_TABLE_VERSION_INTERVAL_SECONDS_DEFAULT
    )
    toTimeInSeconds(intervalStr, QUERY_TABLE_VERSION_INTERVAL_SECONDS)
  }

  def limitPushdownEnabled(conf: SQLConf): Boolean = {
    conf.getConfString(LIMIT_PUSHDOWN_ENABLED_CONF, LIMIT_PUSHDOWN_ENABLED_DEFAULT).toBoolean
  }

  def tokenExchangeMaxRetries(conf: Configuration): Int = {
    val numRetries = conf.getInt(OAUTH_RETRIES_CONF, OAUTH_RETRIES_DEFAULT)
    validateNonNeg(numRetries, OAUTH_RETRIES_CONF)
    numRetries
  }

  def tokenExchangeMaxRetries(conf: SQLConf): Int = {
    val numRetries = conf.getConfString(
      OAUTH_RETRIES_CONF, OAUTH_RETRIES_DEFAULT.toString).toInt
    validateNonNeg(numRetries, NUM_RETRIES_CONF)
    numRetries
  }

  def tokenExchangeMaxRetryDurationInSeconds(conf: Configuration): Int = {
    val maxDur = conf.getInt(
      OAUTH_MAX_RETRY_DURATION_CONF, OAUTH_MAX_RETRY_DURATION_SECONDS_DEFAULT)
    validatePositive(maxDur, OAUTH_MAX_RETRY_DURATION_CONF)
    maxDur
  }

  def tokenExchangeMaxRetryDurationInSeconds(conf: SQLConf): Int = {
    val maxDur = conf.getConfString(
      OAUTH_MAX_RETRY_DURATION_CONF, OAUTH_MAX_RETRY_DURATION_SECONDS_DEFAULT.toString).toInt
    validatePositive(maxDur, OAUTH_MAX_RETRY_DURATION_CONF)
    maxDur
  }

  def tokenRenewalThresholdInSeconds(conf: Configuration): Int = {
    val maxDur = conf.getInt(
      OAUTH_EXPIRATION_THRESHOLD_CONF, OAUTH_EXPIRATION_THRESHOLD_SECONDS_DEFAULT)
    validatePositive(maxDur, OAUTH_EXPIRATION_THRESHOLD_CONF)
    maxDur
  }

  def tokenRenewalThresholdInSeconds(conf: SQLConf): Int = {
    val maxDur = conf.getConfString(
      OAUTH_EXPIRATION_THRESHOLD_CONF, OAUTH_EXPIRATION_THRESHOLD_SECONDS_DEFAULT.toString).toInt
    validatePositive(maxDur, OAUTH_EXPIRATION_THRESHOLD_CONF)
    maxDur
  }

  def structuralSchemaMatchingEnabled(conf: SQLConf): Boolean =
    conf.getConfString(STRUCTURAL_SCHEMA_MATCH_CONF, STRUCTURAL_SCHEMA_MATCH_DEFAULT).toBoolean

  def sparkParquetIOCacheEnabled(conf: Configuration): Boolean = {
    conf.getBoolean(
      SPARK_PARQUET_IO_CACHE_CONF, SPARK_PARQUET_IO_CACHE_DEFAULT.toBoolean)
  }

  def sparkParquetIOCacheEnabled(conf: SQLConf): Boolean = {
    conf.getConfString(
      SPARK_PARQUET_IO_CACHE_CONF, SPARK_PARQUET_IO_CACHE_DEFAULT).toBoolean
  }

  def logPreSignedUrlAccessEnabled(conf: Configuration): Boolean = {
    conf.getBoolean(LOG_PRESIGNED_URL_ACCESS_CONF, LOG_PRESIGNED_URL_ACCESS_DEFAULT)
  }

  def logPreSignedUrlAccessEnabled(conf: SQLConf): Boolean = {
    conf.getConfString(
      LOG_PRESIGNED_URL_ACCESS_CONF, LOG_PRESIGNED_URL_ACCESS_DEFAULT.toString).toBoolean
  }

  def versionMismatchCheckEnabled(conf: Configuration): Boolean = {
    conf.getBoolean(VERSION_MISMATCH_CHECK_ENABLED_CONF, VERSION_MISMATCH_CHECK_ENABLED_DEFAULT)
  }

  def versionMismatchCheckEnabled(conf: SQLConf): Boolean = {
    conf.getConfString(
      VERSION_MISMATCH_CHECK_ENABLED_CONF, VERSION_MISMATCH_CHECK_ENABLED_DEFAULT.toString).toBoolean
  }

  private def toTimeInSeconds(timeStr: String, conf: String): Int = {
    val timeInSeconds = JavaUtils.timeStringAs(timeStr, TimeUnit.SECONDS)
    validateNonNeg(timeInSeconds, conf)
    if (conf == QUERY_TABLE_VERSION_INTERVAL_SECONDS &&
      timeInSeconds < MINIMUM_TABLE_VERSION_INTERVAL_SECONDS) {
      throw new IllegalArgumentException(
        conf + s" must not be less than $MINIMUM_TABLE_VERSION_INTERVAL_SECONDS seconds.")
    }
    if (timeInSeconds > Int.MaxValue) {
      throw new IllegalArgumentException(conf + " is too big: " + timeStr)
    }
    timeInSeconds.toInt
  }

  private def validateNonNeg(value: Long, conf: String): Unit = {
    if (value < 0L) {
      throw new IllegalArgumentException(conf + " must not be negative")
    }
  }

  private def validatePositive(value: Int, conf: String): Unit = {
    if (value <= 0) {
      throw new IllegalArgumentException(conf + " must be positive")
    }
  }

  private def validateNonEmpty(value: String, conf: String): Unit = {
    if (value == null || value.isEmpty) {
      throw new IllegalArgumentException(conf + " must be defined")
    }
  }

  private def validatePortNumber(value: Int, conf: String): Unit = {
    if (value <= 0 || value > 65535) {
      throw new IllegalArgumentException(conf + " must be a valid port number")
    }
  }

  case class ProxyConfig(host: String,
                         port: Int,
                         noProxyHosts: Seq[String] = Seq.empty
                        )
}
