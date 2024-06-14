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

package io.delta.sharing.server

import java.io.{ByteArrayOutputStream, File, FileNotFoundException}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.AccessDeniedException
import java.security.MessageDigest
import java.util.concurrent.CompletableFuture
import javax.annotation.Nullable

import scala.collection.JavaConverters._
import scala.util.Try

import com.linecorp.armeria.common.{HttpData, HttpHeaderNames, HttpHeaders, HttpMethod, HttpRequest, HttpResponse, HttpStatus, MediaType, ResponseHeaders, ResponseHeadersBuilder}
import com.linecorp.armeria.common.auth.OAuth2Token
import com.linecorp.armeria.internal.server.ResponseConversionUtil
import com.linecorp.armeria.server.{Server, ServiceRequestContext}
import com.linecorp.armeria.server.annotation.{ConsumesJson, Default, ExceptionHandler, ExceptionHandlerFunction, Get, Head, Param, Post, ProducesJson}
import com.linecorp.armeria.server.auth.AuthService
import io.delta.kernel.exceptions.{KernelException, TableNotFoundException}
import io.delta.standalone.internal.DeltaCDFErrors
import io.delta.standalone.internal.DeltaCDFIllegalArgumentException
import io.delta.standalone.internal.DeltaDataSource
import io.delta.standalone.internal.DeltaSharedTable
import net.sourceforge.argparse4j.ArgumentParsers
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import scalapb.json4s.Printer

import io.delta.sharing.server.DeltaSharedTableLoader
import io.delta.sharing.server.config.ServerConfig
import io.delta.sharing.server.model.{QueryStatus, SingleAction}
import io.delta.sharing.server.protocol._
import io.delta.sharing.server.util.JsonUtils

object ErrorCode {
  val UNSUPPORTED_OPERATION = "UNSUPPORTED_OPERATION"
  val INTERNAL_ERROR = "INTERNAL_ERROR"
  val RESOURCE_DOES_NOT_EXIST = "RESOURCE_DOES_NOT_EXIST"
  val INVALID_PARAMETER_VALUE = "INVALID_PARAMETER_VALUE"
  val MALFORMED_REQUEST = "MALFORMED_REQUEST"
}

/**
 * A special handler to expose the messages of user facing exceptions to the user. By default, all
 * of exception messages will not be in the response.
 */
class DeltaSharingServiceExceptionHandler extends ExceptionHandlerFunction {
  private val logger = LoggerFactory.getLogger(classOf[DeltaSharingServiceExceptionHandler])

  override def handleException(
      ctx: ServiceRequestContext,
      req: HttpRequest,
      cause: Throwable): HttpResponse = {
    cause match {
      // Handle exceptions caused by incorrect requests
      case _: DeltaSharingNoSuchElementException =>
        if (req.method().equals(HttpMethod.HEAD)) {
          HttpResponse.of(
            ResponseHeaders.builder(HttpStatus.NOT_FOUND).build())
        }
        else {
          HttpResponse.of(
            HttpStatus.NOT_FOUND,
            MediaType.JSON_UTF_8,
            JsonUtils.toJson(
              Map(
                "errorCode" -> ErrorCode.RESOURCE_DOES_NOT_EXIST,
                "message" -> cause.getMessage)))
        }
      case _: DeltaSharingUnsupportedOperationException =>
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonUtils.toJson(
            Map(
              "errorCode" -> ErrorCode.UNSUPPORTED_OPERATION,
              "message" -> cause.getMessage)))
      case _: DeltaSharingIllegalArgumentException =>
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonUtils.toJson(
            Map(
              "errorCode" -> ErrorCode.INVALID_PARAMETER_VALUE,
              "message" -> cause.getMessage)))
      case _: KernelException =>
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonUtils.toJson(
            Map(
              "errorCode" -> ErrorCode.MALFORMED_REQUEST,
              "message" -> cause.getMessage)))
      case _: TableNotFoundException =>
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonUtils.toJson(
            Map(
              "errorCode" -> ErrorCode.MALFORMED_REQUEST,
              "message" -> cause.getMessage)))
      case _: DeltaCDFIllegalArgumentException =>
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonUtils.toJson(
            Map(
              "errorCode" -> ErrorCode.INVALID_PARAMETER_VALUE,
              "message" -> cause.getMessage)))
      case _: FileNotFoundException =>
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonUtils.toJson(
            Map(
              "errorCode" -> ErrorCode.RESOURCE_DOES_NOT_EXIST,
              "message" -> "table files missing")))
      case _: AccessDeniedException =>
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonUtils.toJson(
            Map(
              "errorCode" -> ErrorCode.RESOURCE_DOES_NOT_EXIST,
              "message" -> "permission denied")))
      // Handle potential exceptions thrown when Armeria parses the requests.
      // These exceptions happens before `DeltaSharingService` receives the
      // requests so these exceptions should never contain sensitive information
      // and should be okay to return their messages to the user.
      //
      // valid json but may not be incorect field type
      case (_: scalapb.json4s.JsonFormatException |
      // invalid json
            _: com.fasterxml.jackson.databind.JsonMappingException) =>
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonUtils.toJson(
            Map(
              "errorCode" -> ErrorCode.MALFORMED_REQUEST,
              "message" -> cause.getMessage)))
      case _: NumberFormatException =>
        // `maxResults`/`maxFiles` is not an int.
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.JSON_UTF_8,
          JsonUtils.toJson(
            Map(
              "errorCode" -> ErrorCode.INVALID_PARAMETER_VALUE,
              "message" -> "expected a number but the string didn't have the appropriate format")))
      // Handle unhandled exceptions
      case _ =>
        logger.error(cause.getMessage, cause)
        // Hide message in response
        HttpResponse.of(
          HttpStatus.INTERNAL_SERVER_ERROR,
          MediaType.PLAIN_TEXT_UTF_8,
          JsonUtils.toJson(
            Map(
              "errorCode" -> ErrorCode.INTERNAL_ERROR,
              "message" -> "")))
    }
  }
}

@ExceptionHandler(classOf[DeltaSharingServiceExceptionHandler])
class DeltaSharingService(serverConfig: ServerConfig) {
  import DeltaSharingService._

  private val rand = new scala.util.Random()

  private val sharedTableManager = new SharedTableManager(serverConfig)

  private val deltaSharedTableLoader = new DeltaSharedTableLoader(serverConfig)

  private val logger = LoggerFactory.getLogger(classOf[DeltaSharingService])
  /**
   * Call `func` and catch any unhandled exception and convert it to `DeltaInternalException`. Any
   * code that processes requests should use this method to ensure that unhandled exceptions are
   * always wrapped by `DeltaInternalException`.
   */
  private def processRequest[T](func: => T): T = {
    try func catch {
      case e: DeltaSharingUnsupportedOperationException => throw e
      case e: DeltaSharingIllegalArgumentException => throw e
      case e: DeltaSharingNoSuchElementException => throw e
      case e: DeltaCDFIllegalArgumentException => throw e
      case e: FileNotFoundException => throw e
      case e: AccessDeniedException => throw e
      case e: KernelException => throw e
      case e: TableNotFoundException => throw e
      case e: Throwable => throw new DeltaInternalException(e)
    }
  }

  @Get("/shares")
  @ProducesJson
  def listShares(
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String): ListSharesResponse = processRequest {
    val (shares, nextPageToken) = sharedTableManager.listShares(Option(pageToken), Some(maxResults))
    ListSharesResponse(shares, nextPageToken)
  }

  @Get("/shares/{share}")
  @ProducesJson
  def getShare(@Param("share") share: String): GetShareResponse = processRequest {
    GetShareResponse(share = Some(sharedTableManager.getShare(share)))
  }

  @Get("/shares/{share}/schemas")
  @ProducesJson
  def listSchemas(
      @Param("share") share: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String): ListSchemasResponse = processRequest {
    val (schemas, nextPageToken) =
      sharedTableManager.listSchemas(share, Option(pageToken), Some(maxResults))
    ListSchemasResponse(schemas, nextPageToken)
  }

  @Get("/shares/{share}/schemas/{schema}/tables")
  @ProducesJson
  def listTables(
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String): ListTablesResponse = processRequest {
    val (tables, nextPageToken) =
      sharedTableManager.listTables(share, schema, Option(pageToken), Some(maxResults))
    ListTablesResponse(tables, nextPageToken)
  }

  @Get("/shares/{share}/all-tables")
  @ProducesJson
  def listAllTables(
      @Param("share") share: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String): ListAllTablesResponse = processRequest {
    val (tables, nextPageToken) =
      sharedTableManager.listAllTables(share, Option(pageToken), Some(maxResults))
    ListAllTablesResponse(tables, nextPageToken)
  }

  private def createHeadersBuilderForTableVersion(version: Long): ResponseHeadersBuilder = {
    ResponseHeaders.builder(200).set(DELTA_TABLE_VERSION_HEADER, version.toString)
  }

  // TODO: deprecate HEAD request in favor of the GET request
  @Head("/shares/{share}/schemas/{schema}/tables/{table}")
  @Get("/shares/{share}/schemas/{schema}/tables/{table}/version")
  def getTableVersion(
    @Param("share") share: String,
    @Param("schema") schema: String,
    @Param("table") table: String,
    @Param("startingTimestamp") @Nullable startingTimestamp: String
  ): HttpResponse = processRequest {
    val tableConfig = sharedTableManager.getTable(share, schema, table)
    if (startingTimestamp != null && !tableConfig.historyShared) {
      throw new DeltaSharingIllegalArgumentException("Reading table by version or timestamp is" +
        " not supported because history sharing is not enabled on table: " +
        s"$share.$schema.$table")
    }
    val version = deltaSharedTableLoader.loadTable(tableConfig, useKernel = true).getTableVersion(
      Option(startingTimestamp)
    )
    if (startingTimestamp != null && version < tableConfig.startVersion) {
      throw new DeltaSharingIllegalArgumentException(
        s"You can only query table data since version ${tableConfig.startVersion}." +
        s"The provided timestamp($startingTimestamp) corresponds to $version."
      )
    }
    val headers = createHeadersBuilderForTableVersion(version).build()
    HttpResponse.of(headers)
  }

  private def getDeltaSharingCapabilitiesMap(headerString: String): Map[String, String] = {
    if (headerString == null) {
      return Map.empty[String, String]
    }
    headerString.toLowerCase().split(";")
      .map(_.split("="))
      .filter(_.size == 2)
      .map { splits =>
        (splits(0), splits(1))
      }.toMap
  }

  @Get("/shares/{share}/schemas/{schema}/tables/{table}/metadata")
  def getMetadata(
      req: HttpRequest,
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("table") table: String,
      @Param("version") @Nullable version: java.lang.Long,
      @Param("timestamp") @Nullable timestamp: String): HttpResponse = processRequest {
    import scala.collection.JavaConverters._
    if (version != null && timestamp != null) {
      throw new DeltaSharingIllegalArgumentException(ErrorStrings.multipleParametersSetErrorMsg(
        Seq("version", "timestamp"))
      )
    }
    if (version != null && version < 0) {
      throw new DeltaSharingIllegalArgumentException("version cannot be negative.")
    }
    val capabilitiesMap = getDeltaSharingCapabilitiesMap(
      req.headers().get(DELTA_SHARING_CAPABILITIES_HEADER)
    )
    val tableConfig = sharedTableManager.getTable(share, schema, table)
    if ((version != null || timestamp != null) && !tableConfig.historyShared) {
      throw new DeltaSharingIllegalArgumentException("Reading table by version or timestamp is" +
        " not supported because history sharing is not enabled on table: " +
        s"$share.$schema.$table")
    }
    val responseFormatSet = getResponseFormatSet(capabilitiesMap)
    val queryResult = deltaSharedTableLoader.loadTable(tableConfig).query(
      includeFiles = false,
      predicateHints = Nil,
      jsonPredicateHints = None,
      limitHint = None,
      version = Option(version).map(_.toLong),
      timestamp = Option(timestamp),
      startingVersion = None,
      endingVersion = None,
      maxFiles = None,
      pageToken = None,
      includeRefreshToken = false,
      refreshToken = None,
      responseFormatSet = responseFormatSet)
    streamingOutput(Some(queryResult.version), queryResult.responseFormat, queryResult.actions)
  }


  @Post("/shares/{share}/schemas/{schema}/tables/{table}/queries/{queryId}")
  @ConsumesJson
  def getQueryStatus(
     req: HttpRequest,
     @Param("share") share: String,
     @Param("schema") schema: String,
     @Param("table") table: String,
     @Param("queryId") queryId: String,
     request: GetQueryInfoRequest): HttpResponse = processRequest {

    if (table == "tableWithAsyncQueryError") {
      throw new DeltaSharingIllegalArgumentException("expected error")
    }

    // simulate async query with 50% chance of return
    // asynchronously client should be able to hand both cases
    // this is for test purpose only and shouldn't cause flakiness
    if(rand.nextInt(100) > 50) {
        streamingOutput(
          Some(0),
          "parquet",
          Seq(
            SingleAction(queryStatus = QueryStatus(queryId))
          )
        )
      } else {

      // we are reusing the table here to simulate a view query result
      val tableConfig = sharedTableManager.getTable(share, schema, table)
      val capabilitiesMap = getDeltaSharingCapabilitiesMap(
        req.headers().get(DELTA_SHARING_CAPABILITIES_HEADER))
      val responseFormatSet = getResponseFormatSet(capabilitiesMap)
      val queryResult = deltaSharedTableLoader.loadTable(tableConfig).query(
        includeFiles = true,
        Seq.empty[String],
        None,
        None,
        None,
        None,
        None,
        None,
        maxFiles = request.maxFiles,
        pageToken = request.pageToken,
        false,
        None,
        responseFormatSet = responseFormatSet)
      if (queryResult.version < tableConfig.startVersion) {
        throw new DeltaSharingIllegalArgumentException(
          s"You can only query table data since version ${tableConfig.startVersion}."
        )
      }

      streamingOutput(Some(queryResult.version), queryResult.responseFormat, queryResult.actions)
    }
  }

  @Post("/shares/{share}/schemas/{schema}/tables/{table}/query")
  @ConsumesJson
  def listFiles(
      req: HttpRequest,
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("table") table: String,
      request: QueryTableRequest): HttpResponse = processRequest {
    val capabilitiesMap = getDeltaSharingCapabilitiesMap(
      req.headers().get(DELTA_SHARING_CAPABILITIES_HEADER)
    )
    val numVersionParams =
      Seq(request.version, request.timestamp, request.startingVersion).count(_.isDefined)
    if (numVersionParams > 1) {
      throw new DeltaSharingIllegalArgumentException(ErrorStrings.multipleParametersSetErrorMsg(
        Seq("version", "timestamp", "startingVersion"))
      )
    }
    if (request.version.isDefined && request.version.get < 0) {
      throw new DeltaSharingIllegalArgumentException("table version cannot be negative.")
    }
    if (request.startingVersion.isDefined && request.startingVersion.get < 0) {
      throw new DeltaSharingIllegalArgumentException("startingVersion cannot be negative.")
    }
    if (request.maxFiles.exists(_ <= 0)) {
      throw new DeltaSharingIllegalArgumentException("maxFiles must be positive.")
    }
    if (numVersionParams > 0 && request.includeRefreshToken.contains(true)) {
      throw new DeltaSharingIllegalArgumentException(
        "includeRefreshToken cannot be used when querying a specific version."
      )
    }
    if (request.pageToken.isDefined && request.includeRefreshToken.contains(true)) {
      throw new DeltaSharingIllegalArgumentException(
        "includeRefreshToken must be used in the first page request."
      )
    }
    if (numVersionParams > 0 && request.refreshToken.isDefined) {
      throw new DeltaSharingIllegalArgumentException(
        "refreshToken cannot be used when querying a specific version."
      )
    }
    if (request.pageToken.isDefined && request.refreshToken.isDefined) {
      throw new DeltaSharingIllegalArgumentException(
        "refreshToken must be used in the first page request."
      )
    }

    if(getAsyncQuery(capabilitiesMap) && !request.idempotencyKey.isDefined) {
      throw new DeltaSharingIllegalArgumentException(
        "idempotency_key is required for async query."
      )
    }

    val start = System.currentTimeMillis

    if(getAsyncQuery(capabilitiesMap)) {
      val queryId = s"${share}_${schema}_${table}"

      streamingOutput(
        Some(0),
        "parquet",
        Seq(
          SingleAction(queryStatus = QueryStatus(queryId))
        )
      )
    } else {
      val tableConfig = sharedTableManager.getTable(share, schema, table)
      if (numVersionParams > 0) {
        if (!tableConfig.historyShared) {
          throw new DeltaSharingIllegalArgumentException(
            "Reading table by version or " +
            "timestamp is not supported because history sharing is not enabled on table: " +
            s"$share.$schema.$table")
        }
        if (request.version.exists(_ < tableConfig.startVersion) ||
          request.startingVersion.exists(_ < tableConfig.startVersion)) {
          throw new DeltaSharingIllegalArgumentException(
            s"You can only query table data since version ${tableConfig.startVersion}."
          )
        }
        if (request.endingVersion.isDefined &&
          request.startingVersion.exists(_ > request.endingVersion.get)) {
          throw new DeltaSharingIllegalArgumentException(
            s"startingVersion(${request.startingVersion.get}) must be smaller than or equal to " +
              s"endingVersion(${request.endingVersion.get})."
          )
        }
      }
      val responseFormatSet = getResponseFormatSet(capabilitiesMap)
      val queryResult = deltaSharedTableLoader.loadTable(tableConfig).query(
        includeFiles = true,
        request.predicateHints,
        request.jsonPredicateHints,
        request.limitHint,
        request.version,
        request.timestamp,
        request.startingVersion,
        request.endingVersion,
        request.maxFiles,
        request.pageToken,
        request.includeRefreshToken.getOrElse(false),
        request.refreshToken,
        responseFormatSet = responseFormatSet)
      if (queryResult.version < tableConfig.startVersion) {
        throw new DeltaSharingIllegalArgumentException(
          s"You can only query table data since version ${tableConfig.startVersion}."
        )
      }
      logger.info(s"Took ${System.currentTimeMillis - start} ms to load the table " +
        s"and sign ${queryResult.actions.length - 2} urls for table $share/$schema/$table")
      streamingOutput(Some(queryResult.version), queryResult.responseFormat, queryResult.actions)
    }
  }

  // scalastyle:off argcount
  @Get("/shares/{share}/schemas/{schema}/tables/{table}/changes")
  @ConsumesJson
  def listCdfFiles(
      req: HttpRequest,
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("table") table: String,
      @Param("startingVersion") @Nullable startingVersion: String,
      @Param("endingVersion") @Nullable endingVersion: String,
      @Param("startingTimestamp") @Nullable startingTimestamp: String,
      @Param("endingTimestamp") @Nullable endingTimestamp: String,
      @Param("includeHistoricalMetadata") @Nullable includeHistoricalMetadata: String,
      @Param("maxFiles") @Nullable maxFiles: java.lang.Integer,
      @Param("pageToken") @Nullable pageToken: String
  ): HttpResponse = processRequest {
    // scalastyle:on argcount
    if (maxFiles != null && maxFiles <= 0) {
      throw new DeltaSharingIllegalArgumentException("maxFiles must be positive.")
    }
    val capabilitiesMap = getDeltaSharingCapabilitiesMap(
      req.headers().get(DELTA_SHARING_CAPABILITIES_HEADER)
    )
    val start = System.currentTimeMillis
    val tableConfig = sharedTableManager.getTable(share, schema, table)
    if (!tableConfig.historyShared) {
      throw new DeltaSharingIllegalArgumentException("cdf is not enabled on table " +
        s"$share.$schema.$table")
    }

    val responseFormatSet = getResponseFormatSet(capabilitiesMap)
    val queryResult = deltaSharedTableLoader.loadTable(tableConfig).queryCDF(
      getCdfOptionsMap(
        Option(startingVersion),
        Option(endingVersion),
        Option(startingTimestamp),
        Option(endingTimestamp)
      ),
      includeHistoricalMetadata = Try(includeHistoricalMetadata.toBoolean).getOrElse(false),
      Option(maxFiles).map(_.toInt),
      Option(pageToken),
      responseFormatSet = responseFormatSet
    )
    logger.info(s"Took ${System.currentTimeMillis - start} ms to load the table cdf " +
      s"and sign ${queryResult.actions.length - 2} urls for table $share/$schema/$table")
    streamingOutput(Some(queryResult.version), queryResult.responseFormat, queryResult.actions)
  }

  private def streamingOutput(
      version: Option[Long],
      responseFormat: String,
      actions: Seq[Object]): HttpResponse = {
    val headers = if (version.isDefined) {
      createHeadersBuilderForTableVersion(version.get)
      .set(HttpHeaderNames.CONTENT_TYPE, DELTA_TABLE_METADATA_CONTENT_TYPE)
      .set(DELTA_SHARING_CAPABILITIES_HEADER, s"$DELTA_SHARING_RESPONSE_FORMAT=$responseFormat")
      .build()
    } else {
      ResponseHeaders.builder(200)
      .set(HttpHeaderNames.CONTENT_TYPE, DELTA_TABLE_METADATA_CONTENT_TYPE)
      .set(DELTA_SHARING_CAPABILITIES_HEADER, s"$DELTA_SHARING_RESPONSE_FORMAT=$responseFormat")
      .build()
    }
    ResponseConversionUtil.streamingFrom(
      actions.asJava.stream(),
      headers,
      HttpHeaders.of(),
      (o: Object) => processRequest {
        val out = new ByteArrayOutputStream
        JsonUtils.mapper.writeValue(out, o)
        out.write('\n')
        HttpData.wrap(out.toByteArray)
      },
      ServiceRequestContext.current().blockingTaskExecutor())
  }
}


object DeltaSharingService {
  val DELTA_TABLE_VERSION_HEADER = "Delta-Table-Version"
  val DELTA_TABLE_METADATA_CONTENT_TYPE = "application/x-ndjson; charset=utf-8"
  val DELTA_SHARING_CAPABILITIES_HEADER = "delta-sharing-capabilities"
  val DELTA_SHARING_RESPONSE_FORMAT = "responseformat"
  val DELTA_SHARING_CAPABILITIES_ASYNC_QUERY = "asyncquery"

  private val parser = {
    val parser = ArgumentParsers
      .newFor("Delta Sharing Server")
      .build()
      .defaultHelp(true)
      .description("Start the Delta Sharing Server.")
    parser.addArgument("-c", "--config")
      .required(true)
      .metavar("FILE")
      .dest("config")
      .help("The server config file path")
    parser
  }

  private def updateDefaultJsonPrinterForScalaPbConverterUtil(): Unit = {
    val module = Class.forName("com.linecorp.armeria.server.scalapb.ScalaPbConverterUtil$")
      .getDeclaredField("MODULE$").get(null)
    val defaultJsonPrinterField =
      Class.forName("com.linecorp.armeria.server.scalapb.ScalaPbConverterUtil$")
        .getDeclaredField("defaultJsonPrinter")
    defaultJsonPrinterField.setAccessible(true)
    defaultJsonPrinterField.set(module, new Printer())
  }

  def start(serverConfig: ServerConfig): Server = {
    lazy val server = {
      updateDefaultJsonPrinterForScalaPbConverterUtil()
      val builder = Server.builder()
        .defaultHostname(serverConfig.getHost)
        .disableDateHeader()
        .disableServerHeader()
        .requestTimeout(java.time.Duration.ofSeconds(serverConfig.requestTimeoutSeconds))
        .annotatedService(serverConfig.endpoint, new DeltaSharingService(serverConfig): Any)
      if (serverConfig.ssl == null) {
        builder.http(serverConfig.getPort)
      } else {
        builder.https(serverConfig.getPort)
        if (serverConfig.ssl.selfSigned) {
          builder.tlsSelfSigned()
        } else {
          if (serverConfig.ssl.certificatePasswordFile == null) {
            builder.tls(
              new File(serverConfig.ssl.certificateFile),
              new File(serverConfig.ssl.certificateKeyFile))
          } else {
            builder.tls(
              new File(serverConfig.ssl.certificateFile),
              new File(serverConfig.ssl.certificateKeyFile),
              FileUtils.readFileToString(new File(serverConfig.ssl.certificatePasswordFile), UTF_8)
            )
          }
        }
      }
      if (serverConfig.getAuthorization != null) {
        // Authorization is set. Set up the authorization using the token in the server config.
        val authServiceBuilder =
          AuthService.builder.addOAuth2((_: ServiceRequestContext, token: OAuth2Token) => {
            // Use `MessageDigest.isEqual` to do a time-constant comparison to avoid timing attacks
            val authorized = MessageDigest.isEqual(
              token.accessToken.getBytes(UTF_8),
              serverConfig.getAuthorization.getBearerToken.getBytes(UTF_8))
            CompletableFuture.completedFuture(authorized)
          })
        builder.decorator(authServiceBuilder.newDecorator)
      }
      builder.build()
    }
    server.start().get()
    server
  }

  private def checkCDFOptionsValidity(
    startingVersion: Option[String],
    endingVersion: Option[String],
    startingTimestamp: Option[String],
    endingTimestamp: Option[String]): Unit = {
    // check if we have both version and timestamp parameters
    if (startingVersion.isDefined && startingTimestamp.isDefined) {
      throw DeltaCDFErrors.multipleCDFBoundary("starting")
    }
    if (endingVersion.isDefined && endingTimestamp.isDefined) {
      throw DeltaCDFErrors.multipleCDFBoundary("ending")
    }
    if (startingVersion.isEmpty && startingTimestamp.isEmpty) {
      throw DeltaCDFErrors.noStartVersionForCDF
    }
    if (startingVersion.isDefined) {
      try {
        startingVersion.get.toLong
      } catch {
        case _: NumberFormatException =>
          throw new DeltaCDFIllegalArgumentException("startingVersion is not a valid number.")
      }
    }
    if (endingVersion.isDefined) {
      try {
        endingVersion.get.toLong
      } catch {
        case _: NumberFormatException =>
          throw new DeltaCDFIllegalArgumentException("endingVersion is not a valid number.")
      }
    }
    // startingTimestamp and endingTimestamp are validated in the delta sharing cdc reader.
  }

  private[server] def getCdfOptionsMap(
    startingVersion: Option[String],
    endingVersion: Option[String],
    startingTimestamp: Option[String],
    endingTimestamp: Option[String]): Map[String, String] = {
    checkCDFOptionsValidity(startingVersion, endingVersion, startingTimestamp, endingTimestamp)

    (startingVersion.map(DeltaDataSource.CDF_START_VERSION_KEY -> _) ++
    endingVersion.map(DeltaDataSource.CDF_END_VERSION_KEY -> _) ++
    startingTimestamp.map(DeltaDataSource.CDF_START_TIMESTAMP_KEY -> _) ++
    endingTimestamp.map(DeltaDataSource.CDF_END_TIMESTAMP_KEY -> _)).toMap
  }

  private[server] def getResponseFormatSet(headerCapabilities: Map[String, String]): Set[String] = {
    headerCapabilities.get(DELTA_SHARING_RESPONSE_FORMAT).getOrElse(
      DeltaSharedTable.RESPONSE_FORMAT_PARQUET
    ).split(",").toSet
  }

  private[server] def getAsyncQuery(headerCapabilities: Map[String, String]): Boolean = {
    headerCapabilities.get(DELTA_SHARING_CAPABILITIES_ASYNC_QUERY).exists(_.toBoolean)
  }

  def main(args: Array[String]): Unit = {
    val ns = parser.parseArgsOrFail(args)
    val serverConfigPath = ns.getString("config")
    val serverConf = ServerConfig.load(serverConfigPath)
    start(serverConf).blockUntilShutdown()
  }
}
