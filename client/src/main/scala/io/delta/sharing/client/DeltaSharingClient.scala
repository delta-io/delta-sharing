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

import java.io.{BufferedReader, InputStreamReader}
import java.net.{URL, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.{ISO_DATE, ISO_DATE_TIME}
import java.util.UUID

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.conf.Configuration
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

import io.delta.sharing.client.auth.{AuthConfig, AuthCredentialProviderFactory}
import io.delta.sharing.client.model._
import io.delta.sharing.client.util.{ConfUtils, JsonUtils, RetryUtils, UnexpectedHttpStatus}

/** An interface to fetch Delta metadata from remote server. */
trait DeltaSharingClient {
  def listAllTables(): Seq[Table]

  def getTableVersion(table: Table, startingTimestamp: Option[String] = None): Long

  def getMetadata(
      table: Table,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None): DeltaTableMetadata

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

// A case class containing parameters parsed from the input delta sharing table path with the
// format of "profile_file#share.schema.table".
case class ParsedDeltaSharingTablePath(
    profileFile: String,
    share: String,
    schema: String,
    table: String)

// A case class containing the parsed response of delta sharing rpcs.
case class ParsedDeltaSharingResponse(
    version: Long,
    respondedFormat: String,
    includedEndStreamAction: Option[Boolean],
    lines: Seq[String],
    capabilities: Option[String])

private[sharing] trait PaginationResponse {
  def nextPageToken: Option[String]
}

/**
 * A trait to represent the request object for
 * fetching next page in paginated query.
 * This can be use in both sync and async query.
*/
private[sharing] trait NextPageRequest {
  def maxFiles: Option[Int]
  def pageToken: Option[String]

  // Clone the request object with new optional fields.
  // this is used to generate a new request object when
  // fetching next pages in paginated query.
  def clone(
     maxFiles: Option[Int],
     pageToken: Option[String]): NextPageRequest
}

private[sharing] case class QueryTableRequest(
  predicateHints: Seq[String],
  limitHint: Option[Long],
  version: Option[Long],
  timestamp: Option[String],
  startingVersion: Option[Long],
  endingVersion: Option[Long],
  jsonPredicateHints: Option[String],
  maxFiles: Option[Int],
  pageToken: Option[String],
  includeRefreshToken: Option[Boolean],
  refreshToken: Option[String],
  idempotency_key: Option[String]
) extends NextPageRequest {
  override def clone(
        maxFiles: Option[Int],
        pageToken: Option[String]): NextPageRequest = {
      this.copy(
        maxFiles = maxFiles,
        pageToken = pageToken,
        refreshToken = None,
        includeRefreshToken = None)
  }
}

private[sharing] case class GetQueryTableInfoRequest(
  queryId: String,
  maxFiles: Option[Int],
  pageToken: Option[String]
  ) extends NextPageRequest {
  override def clone(
      maxFiles: Option[Int],
      pageToken: Option[String]): NextPageRequest = {
    this.copy(maxFiles = maxFiles, pageToken = pageToken)
  }
}

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
    readerFeatures: String = "",
    queryTablePaginationEnabled: Boolean = false,
    maxFilesPerReq: Int = 100000,
    includeEndStreamAction: Boolean = true,
    enableAsyncQuery: Boolean = false,
    asyncQueryPollIntervalMillis: Long = 10000L,
    asyncQueryMaxDuration: Long = 600000L,
    tokenExchangeMaxRetries: Int = 5,
    tokenExchangeMaxRetryDurationInSeconds: Int = 60,
    tokenRenewalThresholdInSeconds: Int = 600
  ) extends DeltaSharingClient with Logging {

  logInfo(s"DeltaSharingRestClient with enableAsyncQuery $enableAsyncQuery")

  import DeltaSharingRestClient._

  @volatile private var created = false

  // Convert the responseFormat to a Seq to be used later.
  private val responseFormatSet = responseFormat.split(",").toSet

  private var dsQueryId: Option[String] = None

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

  private lazy val authCredentialProvider = AuthCredentialProviderFactory.createCredentialProvider(
    profileProvider.getProfile,
    AuthConfig(tokenExchangeMaxRetries,
      tokenExchangeMaxRetryDurationInSeconds, tokenRenewalThresholdInSeconds),
    client
  )

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
        s"${RESPONSE_TABLE_VERSION_HEADER_KEY} in the header")
    }
  }

  /**
   * Compare responseFormatSet and respondedFormat, error out when responseFormatSet doesn't contain
   * respondedFormat. The client allows backward compatibility by specifying
   * responseFormat=parquet,delta in the request header.
   */
  private def checkRespondedFormat(respondedFormat: String, rpc: String, table: String): Unit = {
    if (!responseFormatSet.contains(respondedFormat)) {
      logError(s"RespondedFormat($respondedFormat) is different from requested " +
        s"responseFormat($responseFormat) for $rpc for table $table.")
      throw new IllegalArgumentException("The responseFormat returned from the delta sharing " +
        s"server doesn't match the requested responseFormat: respondedFormat($respondedFormat)" +
        s" != requestedFormat($responseFormat).")
    }
  }

  def getMetadata(
      table: Table,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None): DeltaTableMetadata = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val encodedParams = getEncodedMetadataParams(versionAsOf, timestampAsOf)

    val target = getTargetUrl(
      s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName/metadata" +
        s"$encodedParams")
    val response = getNDJson(target)

    checkRespondedFormat(
      response.respondedFormat,
      rpc = "getMetadata",
      table = s"${table.share}.${table.schema}.${table.name}"
    )

    if (response.respondedFormat == RESPONSE_FORMAT_DELTA) {
      return DeltaTableMetadata(
        response.version,
        lines = response.lines,
        respondedFormat = response.respondedFormat
      )
    }

    val protocol = JsonUtils.fromJson[SingleAction](response.lines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](response.lines(1)).metaData
    if (response.lines.size != 2) {
      throw new IllegalStateException("received more than two lines")
    }
    DeltaTableMetadata(
      response.version,
      protocol,
      metadata,
      respondedFormat = response.respondedFormat
    )
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
      jsonPredicateHints: Option[String],
      refreshToken: Option[String]): DeltaTableFiles = {
    // Retrieve refresh token when querying the latest snapshot.
    val includeRefreshToken = versionAsOf.isEmpty && timestampAsOf.isEmpty
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target = getTargetUrl(
      s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName/query")

    val idempotency_key = if (enableAsyncQuery) {
      Some(UUID.randomUUID().toString)
    } else {
      None
    }

    val request: QueryTableRequest = QueryTableRequest(
      predicateHints = predicates,
      limitHint = limit,
      version = versionAsOf,
      timestamp = timestampAsOf,
      startingVersion = None,
      endingVersion = None,
      jsonPredicateHints = jsonPredicateHints,
      maxFiles = None,
      pageToken = None,
      includeRefreshToken = Some(includeRefreshToken),
      refreshToken = refreshToken,
      idempotency_key = idempotency_key
    )

    val updatedRequest = if (queryTablePaginationEnabled) {
        request.copy(
          maxFiles = Some(maxFilesPerReq))
      } else {
        request
      }

    val (version, respondedFormat, lines, refreshTokenOpt) =
      getFilesByPage(table, target, updatedRequest)

    checkRespondedFormat(
      respondedFormat,
      rpc = s"getFiles(versionAsOf-$versionAsOf, timestampAsOf-$timestampAsOf)",
      table = s"${table.share}.${table.schema}.${table.name}"
    )

    if (respondedFormat == RESPONSE_FORMAT_DELTA) {
      return DeltaTableFiles(
        version,
        lines = lines,
        refreshToken = refreshTokenOpt,
        respondedFormat = respondedFormat
      )
    }
    require(versionAsOf.isEmpty || versionAsOf.get == version)
    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
    val files = ArrayBuffer[AddFile]()
    lines.drop(2).foreach { line =>
      val action = JsonUtils.fromJson[SingleAction](line)
      if (action.file != null) {
        files.append(action.file)
      } else {
        throw new IllegalStateException(s"Unexpected Line:${line}")
      }
    }
    DeltaTableFiles(
      version,
      protocol,
      metadata,
      files.toSeq,
      refreshToken = refreshTokenOpt,
      respondedFormat = respondedFormat
    )
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
    val request: QueryTableRequest = QueryTableRequest(
      predicateHints = Nil,
      limitHint = None,
      version = None,
      timestamp = None,
      startingVersion = Some(startingVersion),
      endingVersion = endingVersion,
      jsonPredicateHints = None,
      maxFiles = None,
      pageToken = None,
      includeRefreshToken = None,
      refreshToken = None,
      idempotency_key = None
    )
    val (version, respondedFormat, lines) = if (queryTablePaginationEnabled) {
      logInfo(
        s"Making paginated queryTable from version $startingVersion requests for table " +
        s"${table.share}.${table.schema}.${table.name} with maxFiles=$maxFilesPerReq"
      )
      val (version, respondedFormat, lines, _) = getFilesByPage(table, target, request)
      (version, respondedFormat, lines)
    } else {
      val response = getNDJson(target, request)
      (response.version, response.respondedFormat, response.lines)
    }

    checkRespondedFormat(
      respondedFormat,
      rpc = s"getFiles(startingVersion:$startingVersion, endingVersion:$endingVersion)",
      table = s"${table.share}.${table.schema}.${table.name}"
    )

    if (respondedFormat == RESPONSE_FORMAT_DELTA) {
      return DeltaTableFiles(version, lines = lines, respondedFormat = respondedFormat)
    }
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
      additionalMetadatas = additionalMetadatas.toSeq,
      respondedFormat = respondedFormat
    )
  }

  // Send paginated queryTable requests. Loop internally to fetch and concatenate all pages,
  // then return (version, respondedFormat, actions, refreshToken) tuple.
  private def getFilesByPage(
      table: Table,
      targetUrl: String,
      request: QueryTableRequest): (Long, String, Seq[String], Option[String]) = {
    val allLines = ArrayBuffer[String]()
    val start = System.currentTimeMillis()
    var numPages = 1

    val (version, respondedFormat, lines, queryIdOpt) = if (enableAsyncQuery) {
      getNDJsonWithAsync(table, targetUrl, request)
    } else {
      val response = getNDJson(targetUrl, request)
      (response.version, response.respondedFormat, response.lines, None)
    }

    var (filteredLines, endStreamAction) = maybeExtractEndStreamAction(lines)
    if (endStreamAction.isEmpty) {
      logWarning(
        s"EndStreamAction is not returned in the response for paginated query($dsQueryId)."
      )
    }

    val protocol = filteredLines(0)
    val metadata = filteredLines(1)
    // Extract refresh token if available
    val refreshToken = endStreamAction.flatMap { e =>
      Option(e.refreshToken).flatMap { token =>
        if (token.isEmpty) None else Some(token)
      }
    }
    val minUrlExpirationTimestamp = endStreamAction.flatMap { e =>
      Option(e.minUrlExpirationTimestamp)
    }
    allLines.appendAll(filteredLines)

    // Fetch subsequent pages and concatenate all pages
    while (endStreamAction.isDefined &&
      endStreamAction.get.nextPageToken != null &&
      endStreamAction.get.nextPageToken.nonEmpty) {
      numPages += 1

      val (pagingRequest, nextPageUrl) = if (queryIdOpt.isDefined) {
        (
          GetQueryTableInfoRequest(
            queryId = queryIdOpt.get,
            maxFiles = Some(maxFilesPerReq),
            pageToken = Some(endStreamAction.get.nextPageToken)),
          getQueryInfoTargetUrl(table, queryIdOpt.get)
        )
      } else {
        (
          request.clone(
            maxFiles = Some(maxFilesPerReq),
            pageToken = Some(endStreamAction.get.nextPageToken)),
          targetUrl
        )
      }

      val res = fetchNextPageFiles(
        targetUrl = nextPageUrl,
        requestBody = Some(pagingRequest),
        expectedVersion = version,
        expectedRespondedFormat = respondedFormat,
        expectedProtocol = protocol,
        expectedMetadata = metadata,
        pageNumber = numPages
      )
      allLines.appendAll(res._1)
      endStreamAction = res._2
      if (endStreamAction.isEmpty) {
        logWarning(
          s"EndStreamAction is not returned in the response for paginated query($dsQueryId)."
        )
      }
      // Throw an error if the first page is expiring before we get all pages
      if (minUrlExpirationTimestamp.exists(_ <= System.currentTimeMillis())) {
        throw new IllegalStateException("Unable to fetch all pages before minimum url expiration.")
      }
    }

    // TODO: remove logging once changes are rolled out
    logInfo(s"Took ${System.currentTimeMillis() - start} ms to query $numPages pages " +
      s"of ${allLines.size} files")
    (version, respondedFormat, allLines.toSeq, refreshToken)
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
      val response = getNDJson(target, requireVersion = false)
      (response.version, response.respondedFormat, response.lines)
    }

    checkRespondedFormat(
      respondedFormat,
      rpc = s"getCDFFiles(cdfOptions:$cdfOptions)",
      table = s"${table.share}.${table.schema}.${table.name}."
    )

    // To ensure that it works with delta sharing server that doesn't support the requested format.
    if (respondedFormat == RESPONSE_FORMAT_DELTA) {
      return DeltaTableFiles(version, lines = lines, respondedFormat = respondedFormat)
    }
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
      additionalMetadatas = additionalMetadatas.toSeq,
      respondedFormat = respondedFormat
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
    val response = getNDJson(updatedUrl, requireVersion = false)
    var (filteredLines, endStreamAction) = maybeExtractEndStreamAction(response.lines)
    if (endStreamAction.isEmpty) {
      logWarning(
        s"EndStreamAction is not returned in the response for paginated query($dsQueryId)."
      )
    }
    val protocol = filteredLines(0)
    val metadata = filteredLines(1)
    val minUrlExpirationTimestamp = endStreamAction.flatMap { e =>
      Option(e.minUrlExpirationTimestamp)
    }
    allLines.appendAll(filteredLines)

    // Fetch subsequent pages and concatenate all pages
    while (endStreamAction.isDefined &&
      endStreamAction.get.nextPageToken != null &&
      endStreamAction.get.nextPageToken.nonEmpty) {
      numPages += 1
      updatedUrl =
        s"$targetUrl&maxFiles=$maxFilesPerReq&pageToken=${endStreamAction.get.nextPageToken}"
      val res = fetchNextPageFiles(
        targetUrl = updatedUrl,
        requestBody = None,
        expectedVersion = response.version,
        expectedRespondedFormat = response.respondedFormat,
        expectedProtocol = protocol,
        expectedMetadata = metadata,
        pageNumber = numPages
      )
      allLines.appendAll(res._1)
      endStreamAction = res._2
      if (endStreamAction.isEmpty) {
        logWarning(
          s"EndStreamAction is not returned in the response for paginated query($dsQueryId)."
        )
      }
      // Throw an error if the first page is expiring before we get all pages
      if (minUrlExpirationTimestamp.exists(_ <= System.currentTimeMillis())) {
        throw new IllegalStateException("Unable to fetch all pages before minimum url expiration.")
      }
    }

    // TODO: remove logging once changes are rolled out
    logInfo(
      s"Took ${System.currentTimeMillis() - start} ms to query $numPages pages " +
      s"of ${allLines.size} files"
    )
    (response.version, response.respondedFormat, allLines.toSeq)
  }

  // Send next page query request. Validate the response and return next page files
  // (as original json string) with EndStreamAction. EndStreamAction might be null
  // if it's not returned in the response.
  private def fetchNextPageFiles(
      targetUrl: String,
      requestBody: Option[NextPageRequest],
      expectedVersion: Long,
      expectedRespondedFormat: String,
      expectedProtocol: String,
      expectedMetadata: String,
      pageNumber: Int): (Seq[String], Option[EndStreamAction]) = {
    val start = System.currentTimeMillis()
    val response = if (requestBody.isDefined) {
      getNDJson(targetUrl, requestBody.get)
    } else {
      getNDJson(targetUrl, requireVersion = false)
    }
    logInfo(s"Took ${System.currentTimeMillis() - start} to fetch ${pageNumber}th page " +
      s"of ${response.lines.size} lines.")

    // Validate that version/format/protocol/metadata in the response don't change across pages
    if (response.version != expectedVersion ||
      response.respondedFormat != expectedRespondedFormat ||
      response.lines.size < 2 ||
      response.lines(0) != expectedProtocol ||
      response.lines(1) != expectedMetadata) {
      val errorMsg = s"""
        |Received inconsistent version/format/protocol/metadata across pages.
        |Expected: version $expectedVersion, $expectedRespondedFormat,
        |$expectedProtocol, $expectedMetadata. Actual: version ${response.version},
        |${response.respondedFormat}, ${response.lines}""".stripMargin
      logError(s"Error while fetching next page files at url $targetUrl " +
        s"with body(${JsonUtils.toJson(requestBody.orNull)}: $errorMsg)")
      throw new IllegalStateException(errorMsg)
    }

    // Drop protocol + metadata, then extract endStreamAction if there's any
    maybeExtractEndStreamAction(response.lines.drop(2))
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

  // Get encoded parameters for getMetadata rpc, "version=3" or "timestamp=2023-01-01T00:00:00Z".
  private def getEncodedMetadataParams(
      versionAsOf: Option[Long], timestampAsOf: Option[String]): String = {
    val paramMap = versionAsOf.map("version" -> _.toString).toMap ++
      timestampAsOf.map("timestamp" -> _).toMap
    val params = paramMap.map {
      case (key, value) => s"$key=${URLEncoder.encode(value)}"
    }.mkString("&")
    if (params.nonEmpty) {
      "?" + params
    } else {
      ""
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
      target: String, requireVersion: Boolean = true): ParsedDeltaSharingResponse = {
    val (version, capabilities, lines) = getResponse(new HttpGet(target))
    val (respondedFormat, includedEndStreamAction) = getRespondedHeaders(capabilities)

    val response = ParsedDeltaSharingResponse(
      version = version.getOrElse {
        if (requireVersion) {
          throw new IllegalStateException(s"Cannot find " +
            s"${RESPONSE_TABLE_VERSION_HEADER_KEY} in the header")
        } else {
          0L
        }
      },
      respondedFormat = respondedFormat,
      includedEndStreamAction = includedEndStreamAction,
      lines,
      capabilities = capabilities
    )
    checkEndStreamAction(response)
    response
  }

  private def getQueryInfoTargetUrl(table: Table, queryId: String) = {
    val shareName = URLEncoder.encode(table.share, "UTF-8")
    val schemaName = URLEncoder.encode(table.schema, "UTF-8")
    val tableName = URLEncoder.encode(table.name, "UTF-8")
    val encodedQueryId = URLEncoder.encode(queryId, "UTF-8")
    val target = getTargetUrl(
      s"/shares/$shareName/schemas/$schemaName/tables/$tableName/queries/$encodedQueryId")
    target
  }

  private def getTableQueryInfo(
      table: Table,
      queryId: String,
      maxFiles: Option[Int],
      pageToken: Option[String]): (Long, String, Seq[String]) = {
    val target: String = getQueryInfoTargetUrl(table, queryId)
    val request: GetQueryTableInfoRequest = GetQueryTableInfoRequest(
      queryId = queryId,
      maxFiles = maxFiles,
      pageToken = pageToken)

    val response = getNDJson(target, request)
    (response.version, response.respondedFormat, response.lines)
  }

  /*
  * Check if the query is still pending. The first line of the response will
  * be a query status object if the query is still pending.
  * The method return (queryResultLines, queryId, queryPending) tuple. If the queryPending
  * is false it means the query is finished and the queryResultLines contains the result.
   */
  private def checkQueryPending(lines: Seq[String]): (Seq[String], Option[String], Boolean) = {
    val queryStatus = JsonUtils.fromJson[SingleAction](lines(0)).queryStatus

    if (queryStatus == null) {
      (lines, None, false)
    } else {
      if (queryStatus.queryId == null) {
        throw new IllegalStateException(
          "QueryId is not returned in the first line of the response." + lines(0))
      }

      (lines.drop(1), Some(queryStatus.queryId), true)
    }
  }

  /*
  * Get NDJson data with async query.
  * This method is used when we get the result of table query
  * If the query is async mode and still running, this method
  * will keep polling the query id until the query is finished.
  * and return the result back.
  * If the query is in sync mode it will return the query result
  * directly.
   */
  private def getNDJsonWithAsync(
      table: Table,
      target: String,
      request: QueryTableRequest): (Long, String, Seq[String], Option[String]) = {
    // Initial query to get NDJson data
    val response = getNDJson(target, request)

    // Check if the query is still pending
    var (lines, queryIdOpt, queryPending) = checkQueryPending(response.lines)

    var version = response.version
    var respondedFormat = response.respondedFormat

    val startTime = System.currentTimeMillis()
    // Loop while the query is pending
    while (queryPending) {
      if (System.currentTimeMillis() - startTime > asyncQueryMaxDuration) {
        throw new IllegalStateException("Query is timed out after " +
          s"${asyncQueryMaxDuration} ms. Please try again later.")
      }

      val queryId = queryIdOpt.get
      logInfo(s"Query is still pending. Polling queryId: ${queryId}")
      Thread.sleep(asyncQueryPollIntervalMillis)

      val (currentVersion, currentRespondedFormat, currentLines)
        = getTableQueryInfo(table, queryId, request.maxFiles, request.pageToken)
      val (newLines, returnedQueryId, newQueryPending) = checkQueryPending(currentLines)

      if(newQueryPending && returnedQueryId.get != queryId) {
        throw new IllegalStateException("QueryId is not consistent in the response. " +
          s"Expected: $queryId, Actual: ${returnedQueryId.get}")
      }

      version = currentVersion
      respondedFormat = currentRespondedFormat
      lines = newLines
      queryPending = newQueryPending
    }

    (version, respondedFormat, lines, queryIdOpt)
  }

  private def getNDJson[T: Manifest](target: String, data: T): ParsedDeltaSharingResponse = {
    val httpPost = new HttpPost(target)
    val json = JsonUtils.toJson(data)
    httpPost.setHeader("Content-type", "application/json")
    httpPost.setEntity(new StringEntity(json, UTF_8))
    val (version, capabilities, lines) = getResponse(httpPost)
    val (respondedFormat, includedEndStreamAction) = getRespondedHeaders(capabilities)

    val response = ParsedDeltaSharingResponse(
      version = version.getOrElse {
        throw new IllegalStateException("Cannot find Delta-Table-Version in the header")
      },
      respondedFormat = respondedFormat,
      includedEndStreamAction = includedEndStreamAction,
      lines,
      capabilities = capabilities
    )
    checkEndStreamAction(response)
    response
  }

  private def checkEndStreamAction(response: ParsedDeltaSharingResponse): Unit = {
    if (includeEndStreamAction) {
      // Only perform additional check when includeEndStreamAction = true
      response.includedEndStreamAction match {
        case Some(true) =>
          val lastLineAction = JsonUtils.fromJson[SingleAction](response.lines.last)
          if (lastLineAction.endStreamAction == null) {
            throw new IllegalStateException("Client sets includeEndStreamAction=true in the " +
              s"header, server responded with the header set to true(${response.capabilities}, " +
              s"and ${response.lines.size} lines, and last line parsed as $lastLineAction, " +
              s"for query($dsQueryId).")
          }
        case Some(false) =>
          throw new IllegalStateException(
            "Client sets includeEndStreamAction=true in the header, but server responded with" +
              s" the header set to false(${response.capabilities}), for query($dsQueryId)."
          )
        case None =>
          logWarning(
            s"Client sets includeEndStreamAction=true in the header, but server didn't " +
              s"respond with the header for query($dsQueryId)."
          )
      }
    }
  }

  private def getRespondedHeaders(capabilities: Option[String]): (String, Option[Boolean]) = {
    val capabilitiesMap = parseDeltaSharingCapabilities(capabilities)
    val respondedIESAStrOpt = capabilitiesMap
      .get(DELTA_SHARING_CAPABILITIES_INCLUDE_END_STREAM_ACTION)
    (
      capabilitiesMap.get(RESPONSE_FORMAT).getOrElse(RESPONSE_FORMAT_PARQUET),
      respondedIESAStrOpt.map(_.toBoolean)
    )
  }

  private def parseDeltaSharingCapabilities(capabilities: Option[String]): Map[String, String] = {
    if (capabilities.isEmpty) {
      return Map.empty[String, String]
    }
    capabilities.get.toLowerCase().split(DELTA_SHARING_CAPABILITIES_DELIMITER)
      .map(_.split("="))
      .filter(_.size == 2)
      .map { splits =>
        (splits(0), splits(1))
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

  private def tokenExpired(): Boolean = {
    authCredentialProvider.isExpired()
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
      HttpHeaders.USER_AGENT -> getUserAgent(),
      DELTA_SHARING_CAPABILITIES_HEADER -> constructDeltaSharingCapabilities()
    ) ++ customeHeaders
    headers.foreach(header => httpRequest.setHeader(header._1, header._2))
    authCredentialProvider.addAuthHeader(httpRequest)

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
    // Reset dsQueryId before calling RetryUtils, and before prepareHeaders.
    dsQueryId = Some(UUID.randomUUID().toString().split('-').head)
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
          if (statusCode == HttpStatus.SC_UNAUTHORIZED && tokenExpired()) {
            additionalErrorInfo = s"It may be caused by an expired token as it has expired " +
              s"at ${authCredentialProvider.getExpirationTime()}"
          }
          // Only show the last 100 lines in the error to keep it contained.
          val responseToShow = lines.drop(lines.size - 100).mkString("\n")
          throw new UnexpectedHttpStatus(
            s"HTTP request failed with status: $status $responseToShow. $additionalErrorInfo",
            statusCode)
        }
        (
          Option(
            response.getFirstHeader(RESPONSE_TABLE_VERSION_HEADER_KEY)
          ).map(_.getValue.toLong),
          Option(
            response.getFirstHeader(DELTA_SHARING_CAPABILITIES_HEADER)
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
      SPARK_STRUCTURED_STREAMING
    } else {
      "Delta-Sharing-Spark"
    }
    s"$sparkAgent/$VERSION" + s" $sparkVersionString" + s" $getQueryIdString" + USER_AGENT
  }

  private def getQueryIdString: String = {
    s"QueryId-${dsQueryId.getOrElse("not_set")}"
  }

  // The value for delta-sharing-capabilities header, semicolon separated capabilities.
  // Each capability is in the format of "key=value1,value2", values are separated by comma.
  // Example: "capability1=value1;capability2=value3,value4,value5"
  private def constructDeltaSharingCapabilities(): String = {
    var capabilities = Seq[String](s"${RESPONSE_FORMAT}=$responseFormat")
    if (responseFormatSet.contains(RESPONSE_FORMAT_DELTA) && readerFeatures.nonEmpty) {
      capabilities = capabilities :+ s"$READER_FEATURES=$readerFeatures"
    }

    if (enableAsyncQuery) {
     capabilities = capabilities :+ s"$DELTA_SHARING_CAPABILITIES_ASYNC_READ=true"
    }

    if (includeEndStreamAction) {
      capabilities = capabilities :+ s"$DELTA_SHARING_CAPABILITIES_INCLUDE_END_STREAM_ACTION=true"
    }

    val cap = capabilities.mkString(DELTA_SHARING_CAPABILITIES_DELIMITER)
    cap
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
  val READER_FEATURES = "readerfeatures"
  val DELTA_SHARING_CAPABILITIES_ASYNC_READ = "asyncquery"
  val DELTA_SHARING_CAPABILITIES_INCLUDE_END_STREAM_ACTION = "includeEndStreamAction"
  val RESPONSE_FORMAT_DELTA = "delta"
  val RESPONSE_FORMAT_PARQUET = "parquet"
  val DELTA_SHARING_CAPABILITIES_DELIMITER = ";"

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

  /**
   * Parse the user provided path `profile_file#share.schema.table` to
   * ParsedDeltaSharingTablePath.
   */
  def parsePath(path: String): ParsedDeltaSharingTablePath = {
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
    ParsedDeltaSharingTablePath(
      profileFile = profileFile,
      share = tableSplits(0),
      schema = tableSplits(1),
      table = tableSplits(2)
    )
  }

  def apply(
      profileFile: String,
      forStreaming: Boolean = false,
      responseFormat: String = RESPONSE_FORMAT_PARQUET,
      readerFeatures: String = ""
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
    val useAsyncQuery = ConfUtils.useAsyncQuery(sqlConf)
    val asyncQueryMaxDurationMillis = ConfUtils.asyncQueryTimeout(sqlConf)
    val asyncQueryPollDurationMillis = ConfUtils.asyncQueryPollIntervalMillis(sqlConf)

    val tokenExchangeMaxRetries = ConfUtils.tokenExchangeMaxRetries(sqlConf)
    val tokenExchangeMaxRetryDurationInSeconds =
      ConfUtils.tokenExchangeMaxRetryDurationInSeconds(sqlConf)
    val tokenRenewalThresholdInSeconds = ConfUtils.tokenRenewalThresholdInSeconds(sqlConf)

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
        classOf[String],
        classOf[Boolean],
        classOf[Int],
        classOf[Boolean],
        classOf[Long],
        classOf[Long],
        classOf[Int],
        classOf[Int],
        classOf[Int]
    ).newInstance(profileProvider,
        java.lang.Integer.valueOf(timeoutInSeconds),
        java.lang.Integer.valueOf(numRetries),
        java.lang.Long.valueOf(maxRetryDurationMillis),
        java.lang.Boolean.valueOf(sslTrustAll),
        java.lang.Boolean.valueOf(forStreaming),
        responseFormat,
        readerFeatures,
        java.lang.Boolean.valueOf(queryTablePaginationEnabled),
        java.lang.Integer.valueOf(maxFilesPerReq),
        java.lang.Boolean.valueOf(useAsyncQuery),
        java.lang.Long.valueOf(asyncQueryPollDurationMillis),
        java.lang.Long.valueOf(asyncQueryMaxDurationMillis),
        java.lang.Integer.valueOf(tokenExchangeMaxRetries),
        java.lang.Integer.valueOf(tokenExchangeMaxRetryDurationInSeconds),
        java.lang.Integer.valueOf(tokenRenewalThresholdInSeconds)
      ).asInstanceOf[DeltaSharingClient]
  }
}
