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

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.{URL, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.ISO_DATE_TIME
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.util.VersionInfo
import org.apache.http.{HttpHeaders, HttpHost, HttpStatus}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, SSLContextBuilder, TrustSelfSignedStrategy}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{HttpClientBuilder, HttpClients}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import io.delta.sharing.spark.model._
import io.delta.sharing.spark.util.{JsonUtils, RetryUtils, UnexpectedHttpStatus}

/** An interface to fetch Delta metadata from remote server. */
private[sharing] trait DeltaSharingClient {
  def listAllTables(): Seq[Table]

  def getTableVersion(table: Table, startingTimestamp: Option[String] = None): Long

  def getMetadata(table: Table): DeltaTableMetadata

  def getFiles(
    table: Table,
    predicates: Seq[String],
    limit: Option[Long],
    versionAsOf: Option[Long],
    timestampAsOf: Option[String],
    jsonPredicateHints: Option[String],
    refreshToken: Option[String]): DeltaTableFiles

  def getFiles(table: Table, startingVersion: Long, endingVersion: Option[Long]): DeltaTableFiles

  def getCDFFiles(
      table: Table,
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean): DeltaTableFiles

  def getForStreaming(): Boolean = false

  def getProfileProvider: DeltaSharingProfileProvider = null
}

private[sharing] trait PaginationResponse {
  def nextPageToken: Option[String]
}

private[sharing] case class QueryTableRequest(
  predicateHints: Seq[String],
  limitHint: Option[Long],
  version: Option[Long],
  timestamp: Option[String],
  startingVersion: Option[Long],
  endingVersion: Option[Long],
  jsonPredicateHints: Option[String],
  includeRefreshToken: Option[Boolean],
  refreshToken: Option[String]
)

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
    maxRetryDuration: Long = Long.MaxValue,
    sslTrustAll: Boolean = false,
    forStreaming: Boolean = false
  ) extends DeltaSharingClient with Logging {
  import DeltaSharingRestClient._

  @volatile private var created = false

  private var queryId: Option[String] = None

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

  override def getProfileProvider: DeltaSharingProfileProvider = profileProvider

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
    shares.toSeq
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
    tables.toSeq
  }

  override def getForStreaming(): Boolean = forStreaming

  override def getTableVersion(table: Table, startingTimestamp: Option[String] = None): Long = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")

    val encodedParam = if (startingTimestamp.isDefined) {
      s"?startingTimestamp=${URLEncoder.encode(startingTimestamp.get)}"
    } else {
      ""
    }
    val target =
      getTargetUrl(s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/" +
        s"$encodedTableName/version$encodedParam")
    val (version, _) = getResponse(new HttpGet(target), true, true)
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
      limit: Option[Long],
      versionAsOf: Option[Long],
      timestampAsOf: Option[String],
      jsonPredicateHints: Option[String],
      refreshToken: Option[String]): DeltaTableFiles = {
    // Retrieve refresh token when querying the latest snapshot.
    val includeRefreshToken = versionAsOf.isEmpty && timestampAsOf.isEmpty
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target = getTargetUrl(
      s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName/query")
    val (version, lines) = getNDJson(
      target,
      QueryTableRequest(
        predicates,
        limit,
        versionAsOf,
        timestampAsOf,
        None,
        None,
        jsonPredicateHints,
        Some(includeRefreshToken),
        refreshToken
      )
    )
    val (filteredLines, endStreamAction) = maybeExtractEndStreamAction(lines)
    val refreshTokenOpt = endStreamAction.flatMap { e =>
      Option(e.refreshToken).flatMap { token =>
        if (token.isEmpty) None else Some(token)
      }
    }
    if (includeRefreshToken && refreshTokenOpt.isEmpty) {
      logWarning("includeRefreshToken=true but refresh token is not returned.")
    }
    require(versionAsOf.isEmpty || versionAsOf.get == version)
    val protocol = JsonUtils.fromJson[SingleAction](filteredLines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](filteredLines(1)).metaData
    val files = ArrayBuffer[AddFile]()
    filteredLines.drop(2).foreach { line =>
      val action = JsonUtils.fromJson[SingleAction](line)
      if (action.file != null) {
        files.append(action.file)
      } else {
        throw new IllegalStateException(s"Unexpected Line:${line}")
      }
    }
    DeltaTableFiles(version, protocol, metadata, files.toSeq, refreshToken = refreshTokenOpt)
  }

  override def getFiles(
      table: Table,
      startingVersion: Long,
      endingVersion: Option[Long]
  ): DeltaTableFiles = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target = getTargetUrl(
      s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName/query")
    val (version, lines) = getNDJson(
      target,
      QueryTableRequest(
        /* predicateHint */ Nil,
        /* limitHint */ None,
        /* version */ None,
        /* timestamp */ None,
        Some(startingVersion),
        endingVersion,
        /* jsonPredicateHints */ None,
        /* includeRefreshToken */ None,
        /* refreshToken */ None
      )
    )
    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
    val addFiles = ArrayBuffer[AddFileForCDF]()
    val removeFiles = ArrayBuffer[RemoveFile]()
    val additionalMetadatas = ArrayBuffer[Metadata]()
    lines.drop(2).foreach { line =>
      val action = JsonUtils.fromJson[SingleAction](line).unwrap
      action match {
        case a: AddFileForCDF => addFiles.append(a)
        case r: RemoveFile => removeFiles.append(r)
        case m: Metadata => additionalMetadatas.append(m)
        case _ => throw new IllegalStateException(s"Unexpected Line:${line}")
      }
    }
    DeltaTableFiles(
      version,
      protocol,
      metadata,
      addFiles = addFiles.toSeq,
      removeFiles = removeFiles.toSeq,
      additionalMetadatas = additionalMetadatas.toSeq
    )
  }

  override def getCDFFiles(
      table: Table,
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean): DeltaTableFiles = {
    val encodedShare = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchema = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTable = URLEncoder.encode(table.name, "UTF-8")
    val encodedParams = getEncodedCDFParams(cdfOptions, includeHistoricalMetadata)

    val target = getTargetUrl(
      s"/shares/$encodedShare/schemas/$encodedSchema/tables/$encodedTable/changes?$encodedParams")
    val (version, lines) = getNDJson(target, requireVersion = false)
    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData

    val addFiles = ArrayBuffer[AddFileForCDF]()
    val cdfFiles = ArrayBuffer[AddCDCFile]()
    val removeFiles = ArrayBuffer[RemoveFile]()
    val additionalMetadatas = ArrayBuffer[Metadata]()
    lines.drop(2).foreach { line =>
      val action = JsonUtils.fromJson[SingleAction](line).unwrap
      action match {
        case c: AddCDCFile => cdfFiles.append(c)
        case a: AddFileForCDF => addFiles.append(a)
        case r: RemoveFile => removeFiles.append(r)
        case m: Metadata => additionalMetadatas.append(m)
        case _ => throw new IllegalStateException(s"Unexpected Line:${line}")
      }
    }
    DeltaTableFiles(
      version,
      protocol,
      metadata,
      addFiles = addFiles.toSeq,
      cdfFiles = cdfFiles.toSeq,
      removeFiles = removeFiles.toSeq,
      additionalMetadatas = additionalMetadatas.toSeq
    )
  }

  // Check the last line and extract EndStreamAction if there is one.
  private def maybeExtractEndStreamAction(
      lines: Seq[String]): (Seq[String], Option[EndStreamAction]) = {
    val endStreamAction = JsonUtils.fromJson[SingleAction](lines.last).endStreamAction
    if (endStreamAction == null) {
      (lines, None)
    } else {
      (lines.init, Some(endStreamAction))
    }
  }

  private def getEncodedCDFParams(
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean): String = {
    val paramMap = cdfOptions ++ (if (includeHistoricalMetadata) {
      Map("includeHistoricalMetadata" -> "true")
    } else {
      Map.empty
    })
    paramMap.map {
      case (cdfKey, cdfValue) => s"$cdfKey=${URLEncoder.encode(cdfValue)}"
    }.mkString("&")
  }

  private def getNDJson(target: String, requireVersion: Boolean = true): (Long, Seq[String]) = {
    val (version, lines) = getResponse(new HttpGet(target))
    version.getOrElse {
      if (requireVersion) {
        throw new IllegalStateException("Cannot find Delta-Table-Version in the header")
      } else {
        0L
      }
    } -> lines
  }

  private def getNDJson[T: Manifest](target: String, data: T): (Long, Seq[String]) = {
    val httpPost = new HttpPost(target)
    val json = JsonUtils.toJson(data)
    httpPost.setHeader("Content-type", "application/json")
    httpPost.setEntity(new StringEntity(json, UTF_8))
    val (version, lines) = getResponse(httpPost)
    version.getOrElse {
      throw new IllegalStateException("Cannot find Delta-Table-Version in the header")
    } -> lines
  }

  private def getJson[R: Manifest](target: String): R = {
    val (_, response) = getResponse(new HttpGet(target), false, true)
    if (response.size != 1) {
      throw new IllegalStateException(
        "Unexpected response for target: " +  target + ", response=" + response
      )
    }
    JsonUtils.fromJson[R](response(0))
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

  private[spark] def prepareHeaders(httpRequest: HttpRequestBase): HttpRequestBase = {
    val customeHeaders = profileProvider.getCustomHeaders
    if (customeHeaders.contains(HttpHeaders.AUTHORIZATION)
      || customeHeaders.contains(HttpHeaders.USER_AGENT)) {
      throw new IllegalArgumentException(
        s"HTTP header ${HttpHeaders.AUTHORIZATION} and ${HttpHeaders.USER_AGENT} cannot be"
          + "overriden."
      )
    }
    val headers = Map(
      HttpHeaders.AUTHORIZATION -> s"Bearer ${profileProvider.getProfile.bearerToken}",
      HttpHeaders.USER_AGENT -> getUserAgent()
    ) ++ customeHeaders
    headers.foreach(header => httpRequest.setHeader(header._1, header._2))

    httpRequest
  }

  /**
   * Send the http request and return the table version in the header if any, and the response
   * content.
   *
   * The response can be:
   *   - empty if allowNoContent is true.
   *   - single string, if fetchAsOneString is true.
   *   - multi-line response (typically, one per action). This is the default.
   */
  private def getResponse(
      httpRequest: HttpRequestBase,
      allowNoContent: Boolean = false,
      fetchAsOneString: Boolean = false
  ): (Option[Long], Seq[String]) = {
    // Reset queryId before calling RetryUtils, and before prepareHeaders.
    queryId = Some(UUID.randomUUID().toString().split('-').head)
    RetryUtils.runWithExponentialBackoff(numRetries, maxRetryDuration) {
      val profile = profileProvider.getProfile
      val response = client.execute(
        getHttpHost(profile.endpoint),
        prepareHeaders(httpRequest),
        HttpClientContext.create()
      )
      try {
        val status = response.getStatusLine()
        val entity = response.getEntity()
        val lines = if (entity == null) {
          List("")
        } else {
          val input = entity.getContent()
          val lineBuffer = ListBuffer[String]()
          try {
            if (fetchAsOneString) {
              Seq(IOUtils.toString(input, UTF_8))
            } else {
              val reader = new BufferedReader(
                new InputStreamReader(new BoundedInputStream(input), UTF_8)
              )
              var line: Option[String] = None
              while ({
                line = Option(reader.readLine()); line.isDefined
              }) {
                lineBuffer += line.get
              }
              lineBuffer.toList
            }
          } catch {
            case e: org.apache.http.ConnectionClosedException =>
              val error = s"Request to delta sharing server failed due to ${e}."
              logError(error)
              lineBuffer += error
              lineBuffer.toList
          } finally {
            input.close()
          }
        }

        val statusCode = status.getStatusCode
        if (!(statusCode == HttpStatus.SC_OK ||
          (allowNoContent && statusCode == HttpStatus.SC_NO_CONTENT))) {
          var additionalErrorInfo = ""
          if (statusCode == HttpStatus.SC_UNAUTHORIZED && tokenExpired(profile)) {
            additionalErrorInfo = s"It may be caused by an expired token as it has expired " +
              s"at ${profile.expirationTime}"
          }
          // Only show the last 100 lines in the error to keep it contained.
          val responseToShow = lines.drop(lines.size - 100).mkString("\n")
          throw new UnexpectedHttpStatus(
            s"HTTP request failed with status: $status $responseToShow. $additionalErrorInfo",
            statusCode)
        }
        Option(response.getFirstHeader("Delta-Table-Version")).map(_.getValue.toLong) -> lines
      } finally {
        response.close()
      }
    }
  }

  // Add SparkStructuredStreaming in the USER_AGENT header, in order for the delta sharing server
  // to recognize the request for streaming, and take corresponding actions.
  private def getUserAgent(): String = {
    val sparkAgent = if (forStreaming) {
      DeltaSharingRestClient.SPARK_STRUCTURED_STREAMING
    } else {
      "Delta-Sharing-Spark"
    }
    s"$sparkAgent/$VERSION" + s" $sparkVersionString" + s" $getQueryIdString" + USER_AGENT
  }

  private def getQueryIdString: String = {
    s"QueryId-${queryId.getOrElse("not_set")}"
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

  val SPARK_STRUCTURED_STREAMING = "Delta-Sharing-SparkStructuredStreaming"

  lazy val USER_AGENT = {
    try {
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
