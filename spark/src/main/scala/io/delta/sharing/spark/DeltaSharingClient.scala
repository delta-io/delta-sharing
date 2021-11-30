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

import java.net.{URL, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.ISO_DATE_TIME

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.io.IOUtils
import org.apache.hadoop.util.VersionInfo
import org.apache.http.{HttpHeaders, HttpHost, HttpStatus}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpHead, HttpPost, HttpRequestBase}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, SSLContextBuilder, TrustSelfSignedStrategy}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{HttpClientBuilder, HttpClients}
import org.apache.spark.internal.Logging

import io.delta.sharing.spark.model._
import io.delta.sharing.spark.util.{JsonUtils, RetryUtils, UnexpectedHttpStatus}

/** An interface to fetch Delta metadata from remote server. */
private[sharing] trait DeltaSharingClient {
  def listAllTables(): Seq[Table]

  def getTableVersion(table: Table): Long

  def getMetadata(table: Table): DeltaTableMetadata

  def getFiles(table: Table, predicates: Seq[String], limit: Option[Long]): DeltaTableFiles
}

private[sharing] trait PaginationResponse {
  def nextPageToken: Option[String]
}

private[sharing] case class QueryTableRequest(predicateHints: Seq[String], limitHint: Option[Long])

private[sharing] case class ListSharesResponse(
    items: Seq[Share],
    nextPageToken: Option[String]) extends PaginationResponse

private[sharing] case class ListAllTablesResponse(
    items: Seq[Table],
    nextPageToken: Option[String]) extends PaginationResponse

/** A REST client to fetch Delta metadata from remote server. */
private[spark] class DeltaSharingRestClient(
    profileProvider: DeltaSharingProfileProvider,
    timeoutInSeconds: Int = 120,
    numRetries: Int = 10,
    sslTrustAll: Boolean = false) extends DeltaSharingClient {

  @volatile private var created = false

  private lazy val client = {
    val clientBuilder: HttpClientBuilder = if (sslTrustAll) {
      val sslBuilder = new SSLContextBuilder()
        .loadTrustMaterial(null, new TrustSelfSignedStrategy())
      val sslsf = new SSLConnectionSocketFactory(
        sslBuilder.build(),
        SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER
      )
      HttpClients.custom().setSSLSocketFactory(sslsf)
    } else {
      HttpClientBuilder.create()
    }
    val config = RequestConfig.custom()
      .setConnectTimeout(timeoutInSeconds * 1000)
      .setConnectionRequestTimeout(timeoutInSeconds * 1000)
      .setSocketTimeout(timeoutInSeconds * 1000).build()
    val client = clientBuilder
      // Disable the default retry behavior because we have our own retry logic.
      // See `RetryUtils.runWithExponentialBackoff`.
      .disableAutomaticRetries()
      .setDefaultRequestConfig(config)
      .build()
    created = true
    client
  }

  override def listAllTables(): Seq[Table] = {
    listShares().flatMap(listAllTablesInShare)
  }

  private def getTargetUrl(suffix: String): String = {
    s"${profileProvider.getProfile.endpoint.stripSuffix("/")}/${suffix.stripPrefix("/")}"
  }

  private def listShares(): Seq[Share] = {
    val target = getTargetUrl("shares")
    val shares = ArrayBuffer[Share]()
    var response = getJson[ListSharesResponse](target)
    if (response != null && response.items != null) {
      shares ++= response.items
    }
    while (response.nextPageToken.nonEmpty) {
      val encodedPageToken = URLEncoder.encode(response.nextPageToken.get, "UTF-8")
      val target = getTargetUrl(s"/shares?pageToken=$encodedPageToken")
      response = getJson[ListSharesResponse](target)
      if (response != null && response.items != null) {
        shares ++= response.items
      }
    }
    shares
  }

  private def listAllTablesInShare(share: Share): Seq[Table] = {
    val encodedShareName = URLEncoder.encode(share.name, "UTF-8")
    val target = getTargetUrl(s"/shares/$encodedShareName/all-tables")
    val tables = ArrayBuffer[Table]()
    var response = getJson[ListAllTablesResponse](target)
    if (response != null && response.items != null) {
      tables ++= response.items
    }
    while (response.nextPageToken.nonEmpty) {
      val encodedPageToken = URLEncoder.encode(response.nextPageToken.get, "UTF-8")
      val target =
        getTargetUrl(s"/shares/$encodedShareName/all-tables?pageToken=$encodedPageToken")
      response = getJson[ListAllTablesResponse](target)
      if (response != null && response.items != null) {
        tables ++= response.items
      }
    }
    tables
  }

  override def getTableVersion(table: Table): Long = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target =
      getTargetUrl(s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName")
    val (version, _) = getResponse(new HttpHead(target))
    version.getOrElse {
      throw new IllegalStateException("Cannot find Delta-Table-Version in the header")
    }
  }

  def getMetadata(table: Table): DeltaTableMetadata = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target = getTargetUrl(
      s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName/metadata")
    val (version, lines) = getNDJson(target)
    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
    if (lines.size != 2) {
      throw new IllegalStateException("received more than two lines")
    }
    DeltaTableMetadata(version, protocol, metadata)
  }

  private def checkProtocol(protocol: Protocol): Unit = {
    if (protocol.minReaderVersion > DeltaSharingRestClient.CURRENT) {
      throw new IllegalArgumentException(s"The table requires a newer version" +
        s" ${protocol.minReaderVersion} to read. But the current release supports version " +
        s"is ${DeltaSharingProfile.CURRENT} and below. Please upgrade to a newer release.")
    }
  }

  override def getFiles(
      table: Table,
      predicates: Seq[String],
      limit: Option[Long]): DeltaTableFiles = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target = getTargetUrl(
      s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName/query")
    val (version, lines) = getNDJson(target, QueryTableRequest(predicates, limit))
    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
    val files = lines.drop(2).map(line => JsonUtils.fromJson[SingleAction](line).file)
    DeltaTableFiles(version, protocol, metadata, files)
  }

  private def getNDJson(target: String): (Long, Seq[String]) = {
    val (version, response) = getResponse(new HttpGet(target))
    version.getOrElse {
      throw new IllegalStateException("Cannot find Delta-Table-Version in the header")
    } -> response.split("[\n\r]+")
  }

  private def getNDJson[T: Manifest](target: String, data: T): (Long, Seq[String]) = {
    val httpPost = new HttpPost(target)
    val json = JsonUtils.toJson(data)
    httpPost.setHeader("Content-type", "application/json")
    httpPost.setEntity(new StringEntity(json, UTF_8))
    val (version, response) = getResponse(httpPost)
    version.getOrElse {
      throw new IllegalStateException("Cannot find Delta-Table-Version in the header")
    } -> response.split("[\n\r]+")
  }

  private def getJson[R: Manifest](target: String): R = {
    val (_, response) = getResponse(new HttpGet(target))
    JsonUtils.fromJson[R](response)
  }

  private def getHttpHost(endpoint: String): HttpHost = {
    val url = new URL(endpoint)
    val protocol = url.getProtocol
    val port = if (url.getPort == -1) {
      if (protocol == "https") 443 else 80
    } else {
      url.getPort
    }
    new HttpHost(url.getHost, port, protocol)
  }

  private def tokenExpired(profile: DeltaSharingProfile): Boolean = {
    if (profile.expirationTime == null) return false
    try {
      val expirationTime = Timestamp.valueOf(
        LocalDateTime.parse(profile.expirationTime, ISO_DATE_TIME))
      expirationTime.before(Timestamp.valueOf(LocalDateTime.now()))
    } catch {
      case _: Throwable => false
    }
  }

  /**
   * Send the http request and return the table version in the header if any, and the response
   * content.
   */
  private def getResponse(httpRequest: HttpRequestBase): (Option[Long], String) =
    RetryUtils.runWithExponentialBackoff(numRetries) {
      val profile = profileProvider.getProfile
      httpRequest.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer ${profile.bearerToken}")
      httpRequest.setHeader(HttpHeaders.USER_AGENT, DeltaSharingRestClient.USER_AGENT)
      val response =
        client.execute(getHttpHost(profile.endpoint), httpRequest, HttpClientContext.create())
      try {
        val status = response.getStatusLine()
        val entity = response.getEntity()
        val body = if (entity == null) {
          ""
        } else {
          val input = entity.getContent()
          try {
            IOUtils.toString(input, UTF_8)
          } finally {
            input.close()
          }
        }

        val statusCode = status.getStatusCode
        if (statusCode != HttpStatus.SC_OK) {
          var additionalErrorInfo = ""
          if (statusCode == HttpStatus.SC_UNAUTHORIZED && tokenExpired(profile)) {
            additionalErrorInfo = s"It may be caused by an expired token as it has expired " +
              "at ${profile.expirationTime}"
          }
          throw new UnexpectedHttpStatus(
            s"HTTP request failed with status: $status $body. $additionalErrorInfo",
            statusCode)
        }
        Option(response.getFirstHeader("Delta-Table-Version")).map(_.getValue.toLong) -> body
      } finally {
        response.close()
      }
    }

  def close(): Unit = {
    if (created) {
      try client.close() finally created = false
    }
  }

  override def finalize(): Unit = {
    try close() finally super.finalize()
  }
}

private[spark] object DeltaSharingRestClient extends Logging {
  val CURRENT = 1

  lazy val USER_AGENT = {
    try {
      s"Delta-Sharing-Spark/$VERSION" +
        s" $sparkVersionString" +
        s" Hadoop/${VersionInfo.getVersion()}" +
        s" ${spaceFreeProperty("os.name")}/${spaceFreeProperty("os.version")}" +
        s" ${spaceFreeProperty("java.vm.name")}/${spaceFreeProperty("java.vm.version")}" +
        s" java/${spaceFreeProperty("java.version")}" +
        s" scala/${scala.util.Properties.versionNumberString}" +
        s" java_vendor/${spaceFreeProperty("java.vendor")}"
    } catch {
      case e: Throwable =>
        log.warn("Unable to load version information for Delta Sharing", e)
        "Delta-Sharing-Spark/<unknown>"
    }
  }

  /**
   * Return the spark version. When the library is used in Databricks Runtime, it will return
   * Databricks Runtime version.
   */
  def sparkVersionString: String = {
    Option(org.apache.spark.SparkEnv.get).flatMap { env =>
      env.conf.getOption("spark.databricks.clusterUsageTags.sparkVersion")
    }.map(dbrVersion => s"Databricks-Runtime/$dbrVersion")
      .getOrElse(s"Spark/${org.apache.spark.SPARK_VERSION}")
  }

  /**
   * Return the system property using the given key. If the value contains spaces, spaces will be
   * replaced with "_".
   */
  def spaceFreeProperty(key: String): String = {
    val value = System.getProperty(key)
    if (value == null) "<unknown>" else value.replace(' ', '_')
  }
}
