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

import java.io.{ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.concurrent.CompletableFuture
import javax.annotation.Nullable

import scala.collection.JavaConverters._

import com.linecorp.armeria.common.{HttpData, HttpHeaderNames, HttpHeaders, HttpRequest, HttpResponse, HttpStatus, MediaType, ResponseHeaders, ResponseHeadersBuilder}
import com.linecorp.armeria.common.auth.OAuth2Token
import com.linecorp.armeria.internal.server.ResponseConversionUtil
import com.linecorp.armeria.server.{Server, ServiceRequestContext}
import com.linecorp.armeria.server.annotation.{ConsumesJson, Default, ExceptionHandler, ExceptionHandlerFunction, Get, Head, Param, Post, ProducesJson}
import com.linecorp.armeria.server.auth.AuthService
import io.delta.standalone.internal.DeltaSharedTableLoader
import net.sourceforge.argparse4j.ArgumentParsers
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import scalapb.json4s.Printer

import io.delta.sharing.server.config.ServerConfig
import io.delta.sharing.server.model.SingleAction
import io.delta.sharing.server.protocol._
import io.delta.sharing.server.util.JsonUtils

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
        HttpResponse.of(HttpStatus.NOT_FOUND, MediaType.PLAIN_TEXT_UTF_8, cause.getMessage)
      case _: DeltaSharingIllegalArgumentException =>
        HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.PLAIN_TEXT_UTF_8, cause.getMessage)
      // Handle potential exceptions thrown when Armeria parses the requests. These exceptions
      // happens before `DeltaSharingService` receives the requests so these exceptions should never
      // contain sensitive information and should be okay to return their messages to the user.
      case _: scalapb.json4s.JsonFormatException =>
        // valid json but may be incorrect field type
        HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.PLAIN_TEXT_UTF_8, cause.getMessage)
      case _: com.fasterxml.jackson.databind.JsonMappingException =>
        // invalid json
        HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.PLAIN_TEXT_UTF_8, cause.getMessage)
      case _: NumberFormatException =>
        // `maxResults` is not an int.
        HttpResponse.of(
          HttpStatus.BAD_REQUEST,
          MediaType.PLAIN_TEXT_UTF_8,
          "expected a number but the string didn't have the appropriate format")
      // Handle unhandle exceptions
      case _: DeltaInternalException =>
        logger.error(cause.getMessage, cause)
        HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR)
      case _ =>
        logger.error(cause.getMessage, cause)
        ExceptionHandlerFunction.fallthrough()
    }
  }
}

@ExceptionHandler(classOf[DeltaSharingServiceExceptionHandler])
class DeltaSharingService(serverConfig: ServerConfig) {
  import DeltaSharingService.{DELTA_TABLE_VERSION_HEADER, DELTA_TABLE_METADATA_CONTENT_TYPE}

  private val sharedTableManager = new SharedTableManager(serverConfig)

  private val deltaSharedTableLoader = new DeltaSharedTableLoader(serverConfig)

  /**
   * Call `func` and catch any unhandled exception and convert it to `DeltaInternalException`. Any
   * code that processes requests should use this method to ensure that unhandled exceptions are
   * always wrapped by `DeltaInternalException`.
   */
  private def processRequest[T](func: => T): T = {
    try func catch {
      case e: DeltaSharingNoSuchElementException => throw e
      case e: DeltaSharingIllegalArgumentException => throw e
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

  private def createHeadersBuilderForTableVersion(version: Long): ResponseHeadersBuilder = {
    ResponseHeaders.builder(200).set(DELTA_TABLE_VERSION_HEADER, version.toString)
  }

  @Head("/shares/{share}/schemas/{schema}/tables/{table}")
  def getTableVersion(
    @Param("share") share: String,
    @Param("schema") schema: String,
    @Param("table") table: String): HttpResponse = processRequest {
    val tableConfig = sharedTableManager.getTable(share, schema, table)
    val version = deltaSharedTableLoader.loadTable(tableConfig).tableVersion
    val headers = createHeadersBuilderForTableVersion(version).build()
    HttpResponse.of(headers)
  }

  @Get("/shares/{share}/schemas/{schema}/tables/{table}/metadata")
  def getMetadata(
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("table") table: String): HttpResponse = processRequest {
    import scala.collection.JavaConverters._
    val tableConfig = sharedTableManager.getTable(share, schema, table)
    val (version, actions) = deltaSharedTableLoader.loadTable(tableConfig).query(
      includeFiles = false,
      Nil,
      None)
    streamingOutput(version, actions)
  }

  @Post("/shares/{share}/schemas/{schema}/tables/{table}/query")
  @ConsumesJson
  def listFiles(
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("table") table: String,
      queryTableRequest: QueryTableRequest): HttpResponse = processRequest {
    val tableConfig = sharedTableManager.getTable(share, schema, table)
    val (version, actions) = deltaSharedTableLoader.loadTable(tableConfig).query(
      includeFiles = true,
      queryTableRequest.predicateHints,
      queryTableRequest.limitHint)
    streamingOutput(version, actions)
  }

  private def streamingOutput(version: Long, actions: Seq[SingleAction]): HttpResponse = {
    val headers = createHeadersBuilderForTableVersion(version)
      .set(HttpHeaderNames.CONTENT_TYPE, DELTA_TABLE_METADATA_CONTENT_TYPE)
      .build()
    ResponseConversionUtil.streamingFrom(
      actions.asJava.stream(),
      headers,
      HttpHeaders.of(),
      (o: SingleAction) => processRequest {
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

  def main(args: Array[String]): Unit = {
    val ns = parser.parseArgsOrFail(args)
    val serverConfigPath = ns.getString("config")
    val serverConf = ServerConfig.load(serverConfigPath)
    start(serverConf).blockUntilShutdown()
  }
}
