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

package io.delta.sharing.client

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.{URL, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.ISO_DATE_TIME

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.VersionInfo
import org.apache.http.{HttpHeaders, HttpHost, HttpStatus}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpHead, HttpPost, HttpRequestBase}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, SSLContextBuilder, TrustSelfSignedStrategy}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{HttpClientBuilder, HttpClients}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import io.delta.sharing.client.model._
import io.delta.sharing.client.util.{ConfUtils, JsonUtils, RetryUtils, UnexpectedHttpStatus}

/** An interface to fetch Delta metadata from remote server. */
trait DeltaSharingClient {
  def listAllTables(): Seq[Table]

  def getTableVersion(table: Table, startingTimestamp: Option[String] = None): Long

  def getMetadata(table: Table): DeltaTableMetadata

  def getFiles(
    table: Table,
    predicates: Seq[String],
    limit: Option[Long],
    versionAsOf: Option[Long],
    timestampAsOf: Option[String],
    jsonPredicateHints: Option[String]): DeltaTableFiles

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
  maxFiles: Option[Int] = None,
  pageToken: Option[String] = None
)

private[sharing] case class ListSharesResponse(
    items: Seq[Share],
    nextPageToken: Option[String]) extends PaginationResponse

private[sharing] case class ListAllTablesResponse(
    items: Seq[Table],
    nextPageToken: Option[String]) extends PaginationResponse

/** A REST client to fetch Delta metadata from remote server. */
class DeltaSharingRestClient(
    profileProvider: DeltaSharingProfileProvider,
    timeoutInSeconds: Int = 120,
    numRetries: Int = 10,
    maxRetryDuration: Long = Long.MaxValue,
    sslTrustAll: Boolean = false,
    forStreaming: Boolean = false,
    responseFormat: String = DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET,
    queryTablePaginationEnabled: Boolean = false,
    maxFilesPerReq: Int = 100000
  ) extends DeltaSharingClient with Logging {

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
    val (version, _, _) = getResponse(new HttpGet(target), true, true)
    version.getOrElse {
      throw new IllegalStateException(s"Cannot find " +
        s"${DeltaSharingRestClient.RESPONSE_TABLE_VERSION_HEADER_KEY} in the header")
    }
  }

  def getMetadata(table: Table): DeltaTableMetadata = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target = getTargetUrl(
      s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName/metadata")
    val (version, respondedFormat, lines) = getNDJson(target)
    if (responseFormat != respondedFormat) {
      // This could only happen when the asked format is delta and the server doesn't support
      // the requested format.
      logWarning(s"RespondedFormat($respondedFormat) is different from requested responseFormat(" +
        s"$responseFormat) for getMetadata.${table.share}.${table.schema}.${table.name}.")
    }
    // To ensure that it works with delta sharing server that doesn't support the requested format.
    if (respondedFormat == DeltaSharingRestClient.RESPONSE_FORMAT_DELTA) {
      return DeltaTableMetadata(version, lines = lines)
    }

    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
    if (lines.size != 2) {
      throw new IllegalStateException("received more than two lines")
    }
    DeltaTableMetadata(version, protocol, metadata)
  }

  private def checkProtocol(protocol: Protocol): Unit = {
    if (protocol.minReaderVersion > DeltaSharingProfile.CURRENT) {
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
      jsonPredicateHints: Option[String]): DeltaTableFiles = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target = getTargetUrl(
      s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName/query")
    val request = QueryTableRequest(
      predicates,
      limit,
      versionAsOf,
      timestampAsOf,
      None,
      None,
      jsonPredicateHints
    )
    val (version, respondedFormat, lines) = if (queryTablePaginationEnabled) {
      logInfo(
        s"Making paginated queryTable requests for table " +
        s"${table.share}.${table.schema}.${table.name} with maxFiles=$maxFilesPerReq"
      )
      getFilesByPage(target, request)
    } else {
      getNDJson(target, request)
    }

    if (responseFormat != respondedFormat) {
      logWarning(s"RespondedFormat($respondedFormat) is different from requested responseFormat(" +
        s"$responseFormat) for getFiles(versionAsOf-$versionAsOf, timestampAsOf-$timestampAsOf " +
        s"for table ${table.share}.${table.schema}.${table.name}.")
    }
    // To ensure that it works with delta sharing server that doesn't support the requested format.
    if (respondedFormat == DeltaSharingRestClient.RESPONSE_FORMAT_DELTA) {
      return DeltaTableFiles(version, lines = lines)
    }
    require(versionAsOf.isEmpty || versionAsOf.get == version)
    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
    val files = lines.drop(2).map(line => JsonUtils.fromJson[SingleAction](line).file)
    DeltaTableFiles(version, protocol, metadata, files)
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
    val request =
      QueryTableRequest(Nil, None, None, None, Some(startingVersion), endingVersion, None)
    val (version, respondedFormat, lines) = if (queryTablePaginationEnabled) {
      logInfo(
        s"Making paginated queryTable requests for table " +
        s"${table.share}.${table.schema}.${table.name} with maxFiles=$maxFilesPerReq"
      )
      getFilesByPage(target, request)
    } else {
      getNDJson(target, request)
    }
    if (responseFormat != respondedFormat) {
      logWarning(s"RespondedFormat($respondedFormat) is different from requested responseFormat(" +
        s"$responseFormat) for getFiles(startingVersion-$startingVersion, endingVersion-" +
        s"$endingVersion) for table ${table.share}.${table.schema}.${table.name}.")
    }
    // To ensure that it works with delta sharing server that doesn't support the requested format.
    if (respondedFormat == DeltaSharingRestClient.RESPONSE_FORMAT_DELTA) {
      return DeltaTableFiles(version, lines = lines)
    }
    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
    val addFiles = ArrayBuffer[AddFileForCDF]()
    val removeFiles = ArrayBuffer[RemoveFile]()
    val additionalMetadatas = ArrayBuffer[Metadata]()
    lines.drop(2).map(line => JsonUtils.fromJson[SingleAction](line).unwrap).foreach{
      case a: AddFileForCDF => addFiles.append(a)
      case r: RemoveFile => removeFiles.append(r)
      case m: Metadata => additionalMetadatas.append(m)
      case f => throw new IllegalStateException(s"Unexpected File:${f}")
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

  // Send paginated queryTable requests. Loop internally to fetch and concatenate all pages,
  // then return (version, respondedFormat, actions) tuple.
  private def getFilesByPage(
      targetUrl: String,
      request: QueryTableRequest): (Long, String, Seq[String]) = {
    val allLines = ArrayBuffer[String]()
    val start = System.currentTimeMillis()
    var numPages = 1

    // Fetch first page
    var updatedRequest = request.copy(maxFiles = Some(maxFilesPerReq))
    val (version, respondedFormat, lines) = getNDJson(targetUrl, updatedRequest)
    val protocol = lines(0)
    val metadata = lines(1)
    var endAction = JsonUtils.fromJson[SingleAction](lines.last).endStreamAction
    if (endAction == null) {
      logWarning("EndStreamAction is not returned in the response for paginated query.")
    }
    val minUrlExpirationTimestamp = if (endAction != null) {
      Option(endAction.minUrlExpirationTimestamp)
    } else {
      None
    }
    allLines.appendAll(if (endAction != null) lines.init else lines)

    // Fetch subsequent pages and concatenate all pages
    while (endAction != null &&
      endAction.nextPageToken != null &&
      endAction.nextPageToken.nonEmpty) {
      numPages += 1
      updatedRequest = updatedRequest.copy(pageToken = Some(endAction.nextPageToken))
      val res = fetchNextPageFiles(
        targetUrl = targetUrl,
        requestBody = Some(updatedRequest),
        expectedVersion = version,
        expectedRespondedFormat = respondedFormat,
        expectedProtocol = protocol,
        expectedMetadata = metadata
      )
      allLines.appendAll(res._1)
      endAction = res._2
      // Throw an error if the first page is expiring before we get all pages
      if (minUrlExpirationTimestamp.exists(_ <= System.currentTimeMillis())) {
        throw new IllegalStateException("Unable to fetch all pages before minimum url expiration.")
      }
    }

    // TODO: remove logging once changes are rolled out
    logInfo(s"Took ${System.currentTimeMillis() - start} ms to query $numPages pages" +
      s"of ${allLines.size} files")
    (version, respondedFormat, allLines.toSeq)
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
    val (version, respondedFormat, lines) = if (queryTablePaginationEnabled) {
      // TODO: remove logging once changes are rolled out
      logInfo(
        s"Making paginated queryTableChanges requests for table " +
          s"${table.share}.${table.schema}.${table.name} with maxFiles=$maxFilesPerReq"
      )
      getCDFFilesByPage(target)
    } else {
      getNDJson(target, requireVersion = false)
    }
    if (responseFormat != respondedFormat) {
      logWarning(s"RespondedFormat($respondedFormat) is different from requested responseFormat(" +
        s"$responseFormat) for getCDFFiles(cdfOptions-$cdfOptions) for table " +
        s"${table.share}.${table.schema}.${table.name}.")
    }
    // To ensure that it works with delta sharing server that doesn't support the requested format.
    if (respondedFormat == DeltaSharingRestClient.RESPONSE_FORMAT_DELTA) {
      return DeltaTableFiles(version, lines = lines)
    }
    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData

    val addFiles = ArrayBuffer[AddFileForCDF]()
    val cdfFiles = ArrayBuffer[AddCDCFile]()
    val removeFiles = ArrayBuffer[RemoveFile]()
    val additionalMetadatas = ArrayBuffer[Metadata]()
    lines.drop(2).map(line => JsonUtils.fromJson[SingleAction](line).unwrap).foreach{
      case c: AddCDCFile => cdfFiles.append(c)
      case a: AddFileForCDF => addFiles.append(a)
      case r: RemoveFile => removeFiles.append(r)
      case m: Metadata => additionalMetadatas.append(m)
      case f => throw new IllegalStateException(s"Unexpected File:${f}")
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

  // Send paginated queryTableChanges requests. Loop internally to fetch and concatenate all pages,
  // then return (version, respondedFormat, actions) tuple.
  private def getCDFFilesByPage(targetUrl: String): (Long, String, Seq[String]) = {
    val allLines = ArrayBuffer[String]()
    val start = System.currentTimeMillis()
    var numPages = 1

    // Fetch first page
    var updatedUrl = s"$targetUrl&maxFiles=$maxFilesPerReq"
    val (version, respondedFormat, lines) = getNDJson(updatedUrl, requireVersion = false)
    val protocol = lines(0)
    val metadata = lines(1)
    var endAction: EndStreamAction = JsonUtils.fromJson[SingleAction](lines.last).endStreamAction
    if (endAction == null) {
      logWarning("EndStreamAction is not returned in the response for paginated query.")
    }
    val minUrlExpirationTimestamp = if (endAction != null) {
      Option(endAction.minUrlExpirationTimestamp)
    } else {
      None
    }
    allLines.appendAll(if (endAction != null) lines.init else lines)

    // Fetch subsequent pages and concatenate all pages
    while (endAction != null &&
      endAction.nextPageToken != null &&
      endAction.nextPageToken.nonEmpty) {
      numPages += 1
      updatedUrl = s"$targetUrl&maxFiles=$maxFilesPerReq&pageToken=${endAction.nextPageToken}"
      val res = fetchNextPageFiles(
        targetUrl = updatedUrl,
        requestBody = None,
        expectedVersion = version,
        expectedRespondedFormat = respondedFormat,
        expectedProtocol = protocol,
        expectedMetadata = metadata)
      allLines.appendAll(res._1)
      endAction = res._2
      // Throw an error if the first page is expiring before we get all pages
      if (minUrlExpirationTimestamp.exists(_ <= System.currentTimeMillis())) {
        throw new IllegalStateException("Unable to fetch all pages before minimum url expiration.")
      }
    }

    // TODO: remove logging once changes are rolled out
    logInfo(
      s"Took ${System.currentTimeMillis() - start} ms to query $numPages pages" +
      s"of ${allLines.size} files"
    )
    (version, respondedFormat, allLines.toSeq)
  }

  // Send next page query request. Validate the response and return next page files
  // (as original json string) with EndStreamAction. EndStreamAction might be null
  // if it's not returned in the response.
  private def fetchNextPageFiles(
      targetUrl: String,
      requestBody: Option[QueryTableRequest],
      expectedVersion: Long,
      expectedRespondedFormat: String,
      expectedProtocol: String,
      expectedMetadata: String): (Seq[String], EndStreamAction) = {
    val (version, respondedFormat, lines) = if (requestBody.isDefined) {
      getNDJson(targetUrl, requestBody.get)
    } else {
      getNDJson(targetUrl, requireVersion = false)
    }

    // Validate that version/format/protocol/metadata in the response don't change across pages
    if (version != expectedVersion ||
      respondedFormat != expectedRespondedFormat ||
      lines(0) != expectedProtocol ||
      lines(1) != expectedMetadata) {
      val errorMsg = s"""
        |Received inconsistent version/format/protocol/metadata across pages.
        |Expected: version $expectedVersion, $expectedRespondedFormat,
        |$expectedProtocol, $expectedMetadata. Actual: version $version,
        |$respondedFormat, ${lines(0)}, ${lines(1)}""".stripMargin
      logError(s"Error while fetching next page files at url $targetUrl " +
        s"with body(${JsonUtils.toJson(requestBody.orNull)}: $errorMsg)")
      throw new IllegalStateException(errorMsg)
    }

    val endAction = JsonUtils.fromJson[SingleAction](lines.last).endStreamAction
    if (endAction == null) {
      logWarning("EndStreamAction is not returned in the response for paginated query.")
      (lines.drop(2), null)
    } else {
      (lines.drop(2).init, endAction)
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

  private def getNDJson(
      target: String, requireVersion: Boolean = true): (Long, String, Seq[String]) = {
    val (version, capabilities, lines) = getResponse(new HttpGet(target))
    (
      version.getOrElse {
        if (requireVersion) {
          throw new IllegalStateException(s"Cannot find " +
            s"${DeltaSharingRestClient.RESPONSE_TABLE_VERSION_HEADER_KEY} in the header")
        } else {
          0L
        }
      },
      getRespondedFormat(capabilities),
      lines
    )
  }

  private def getNDJson[T: Manifest](target: String, data: T): (Long, String, Seq[String]) = {
    val httpPost = new HttpPost(target)
    val json = JsonUtils.toJson(data)
    httpPost.setHeader("Content-type", "application/json")
    httpPost.setEntity(new StringEntity(json, UTF_8))
    val (version, capabilities, lines) = getResponse(httpPost)
    (
      version.getOrElse {
        throw new IllegalStateException("Cannot find Delta-Table-Version in the header")
      },
      getRespondedFormat(capabilities),
      lines
    )
  }

  private def getRespondedFormat(capabilities: Option[String]): String = {
    val capabilitiesMap = getDeltaSharingCapabilitiesMap(capabilities)
    capabilitiesMap.get(DeltaSharingRestClient.RESPONSE_FORMAT).getOrElse(
      DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
    )
  }
  private def getDeltaSharingCapabilitiesMap(capabilities: Option[String]): Map[String, String] = {
    if (capabilities.isEmpty) {
      return Map.empty[String, String]
    }
    capabilities.get.toLowerCase().split(",").map { capability =>
      val splits = capability.split("=")
      if (splits.size == 2) {
        (splits(0), splits(1))
      } else {
        ("", "")
      }
    }.toMap
  }

  private def getJson[R: Manifest](target: String): R = {
    val (_, _, response) = getResponse(new HttpGet(target), false, true)
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

  private[client] def prepareHeaders(httpRequest: HttpRequestBase): HttpRequestBase = {
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
      HttpHeaders.USER_AGENT -> getUserAgent(),
      DeltaSharingRestClient.DELTA_SHARING_CAPABILITIES_HEADER -> getDeltaSharingCapabilities()
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
  ): (Option[Long], Option[String], Seq[String]) = {
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
          try {
            if (fetchAsOneString) {
              Seq(IOUtils.toString(input, UTF_8))
            } else {
              val reader = new BufferedReader(
                new InputStreamReader(new BoundedInputStream(input), UTF_8)
              )
              var line: Option[String] = None
              val lineBuffer = ListBuffer[String]()
              while ({
                line = Option(reader.readLine()); line.isDefined
              }) {
                lineBuffer += line.get
              }
              lineBuffer.toList
            }
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
        (
          Option(
            response.getFirstHeader(DeltaSharingRestClient.RESPONSE_TABLE_VERSION_HEADER_KEY)
          ).map(_.getValue.toLong),
          Option(
            response.getFirstHeader(DeltaSharingRestClient.DELTA_SHARING_CAPABILITIES_HEADER)
          ).map(_.getValue),
          lines
        )
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
    s"$sparkAgent/$VERSION" + DeltaSharingRestClient.USER_AGENT
  }

  // The value for delta-sharing-capabilities header, semicolon separated capabilities.
  // Each capability is in the format of "key=value1,value2", values are separated by comma.
  // Example: "capability1=value1;capability2=value3,value4,value5"
  private def getDeltaSharingCapabilities(): String = {
    s"${DeltaSharingRestClient.RESPONSE_FORMAT}=$responseFormat"
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

object DeltaSharingRestClient extends Logging {
  val SPARK_STRUCTURED_STREAMING = "Delta-Sharing-SparkStructuredStreaming"
  val DELTA_SHARING_CAPABILITIES_HEADER = "delta-sharing-capabilities"
  val RESPONSE_TABLE_VERSION_HEADER_KEY = "Delta-Table-Version"
  val RESPONSE_FORMAT = "responseformat"
  val RESPONSE_FORMAT_DELTA = "delta"
  val RESPONSE_FORMAT_PARQUET = "parquet"

  lazy val USER_AGENT = {
    try {
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

  /**
   * Parse the user provided path `profile_file#share.schema.share` to
   * `(profile_file, share, schema, share)`.
   */
  def parsePath(path: String): (String, String, String, String) = {
    val shapeIndex = path.lastIndexOf('#')
    if (shapeIndex < 0) {
      throw new IllegalArgumentException(s"path $path is not valid")
    }
    val profileFile = path.substring(0, shapeIndex)
    val tableSplits = path.substring(shapeIndex + 1).split("\\.", -1)
    if (tableSplits.length != 3) {
      throw new IllegalArgumentException(s"path $path is not valid")
    }
    if (profileFile.isEmpty || tableSplits(0).isEmpty ||
      tableSplits(1).isEmpty || tableSplits(2).isEmpty) {
      throw new IllegalArgumentException(s"path $path is not valid")
    }
    (profileFile, tableSplits(0), tableSplits(1), tableSplits(2))
  }

  def apply(
      profileFile: String,
      forStreaming: Boolean = false,
      responseFormat: String = DeltaSharingRestClient.RESPONSE_FORMAT_PARQUET
  ): DeltaSharingClient = {
    val sqlConf = SparkSession.active.sessionState.conf

    val profileProviderClass = ConfUtils.profileProviderClass(sqlConf)
    val profileProvider: DeltaSharingProfileProvider =
      Class.forName(profileProviderClass)
        .getConstructor(classOf[Configuration], classOf[String])
        .newInstance(SparkSession.active.sessionState.newHadoopConf(),
          profileFile)
        .asInstanceOf[DeltaSharingProfileProvider]

    // This is a flag to test the local https server. Should never be used in production.
    val sslTrustAll = ConfUtils.sslTrustAll(sqlConf)
    val numRetries = ConfUtils.numRetries(sqlConf)
    val maxRetryDurationMillis = ConfUtils.maxRetryDurationMillis(sqlConf)
    val timeoutInSeconds = ConfUtils.timeoutInSeconds(sqlConf)
    val queryTablePaginationEnabled = ConfUtils.queryTablePaginationEnabled(sqlConf)
    val maxFilesPerReq = ConfUtils.maxFilesPerQueryRequest(sqlConf)

    val clientClass = ConfUtils.clientClass(sqlConf)
    Class.forName(clientClass)
      .getConstructor(
        classOf[DeltaSharingProfileProvider],
        classOf[Int],
        classOf[Int],
        classOf[Long],
        classOf[Boolean],
        classOf[Boolean],
        classOf[String],
        classOf[Boolean],
        classOf[Int]
      ).newInstance(profileProvider,
        java.lang.Integer.valueOf(timeoutInSeconds),
        java.lang.Integer.valueOf(numRetries),
        java.lang.Long.valueOf(maxRetryDurationMillis),
        java.lang.Boolean.valueOf(sslTrustAll),
        java.lang.Boolean.valueOf(forStreaming),
        responseFormat,
        java.lang.Boolean.valueOf(queryTablePaginationEnabled),
        java.lang.Integer.valueOf(maxFilesPerReq)
      ).asInstanceOf[DeltaSharingClient]
  }
}
