package io.delta.sharing.server

import java.nio.charset.StandardCharsets.UTF_8
import java.io.ByteArrayOutputStream
import java.security.MessageDigest
import java.util.concurrent.CompletableFuture
import com.linecorp.armeria.server.auth.AuthService
import com.linecorp.armeria.common.auth.OAuth2Token
import com.linecorp.armeria.common.{HttpData, HttpHeaderNames, HttpHeaders, HttpRequest, HttpResponse, HttpStatus, ResponseHeaders}
import com.linecorp.armeria.server.{Server, ServiceRequestContext}
import com.linecorp.armeria.server.annotation.{ConsumesJson, Default, ExceptionHandler, ExceptionHandlerFunction, Head, Param, Post}
import io.delta.standalone.internal.DeltaTableHelper
import io.delta.sharing.server.protocol._
import io.delta.sharing.server.config.ServerConfig
import io.delta.sharing.server.util.JsonUtils
import javax.annotation.Nullable
import net.sourceforge.argparse4j.ArgumentParsers
import org.slf4j.LoggerFactory
import scalapb.json4s.Printer
import com.linecorp.armeria.server.annotation.{ProducesJson, Get}

class DeltaSharingServiceExceptionHandler extends ExceptionHandlerFunction {
  private val logger = LoggerFactory.getLogger(classOf[DeltaSharingServiceExceptionHandler])

  override def handleException(
      ctx: ServiceRequestContext,
      req: HttpRequest,
      cause: Throwable): HttpResponse = {
    if (cause.isInstanceOf[NoSuchElementException]) {
      return HttpResponse.of(HttpStatus.NOT_FOUND)
    }
    logger.error(cause.getMessage, cause)
    ExceptionHandlerFunction.fallthrough()
  }
}

@ExceptionHandler(classOf[DeltaSharingServiceExceptionHandler])
class DeltaSharingService(serverConfig: ServerConfig) {
  import DeltaSharingService.{DELTA_TABLE_VERSION_HEADER, DELTA_TABLE_METADATA_CONTENT_TYPE}

  private val shareManagement = new ShareManagement(serverConfig)

  @Get("/shares")
  @ProducesJson
  def listShares(
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String): ListSharesResponse = {
    val (shares, nextPageToken) = shareManagement.listShares(Option(pageToken), Some(maxResults))
    ListSharesResponse(shares, nextPageToken)
  }

  @Get("/shares/{share}/schemas")
  @ProducesJson
  def listSchemas(
      @Param("share") share: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String): ListSchemasResponse = {
    val (schemas, nextPageToken) = shareManagement.listSchemas(share, Option(pageToken), Some(maxResults))
    ListSchemasResponse(schemas, nextPageToken)
  }

  @Head("/shares/{share}/schemas/{schema}/tables/{table}")
  def getTableVersion(
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("table") table: String): HttpResponse = {
    val tableConfig = shareManagement.getTable(share, schema, table)
    val version = DeltaTableHelper.getTableVersion(tableConfig)
    val headers = ResponseHeaders.builder(200)
      .set(DeltaSharingService.DELTA_TABLE_VERSION_HEADER, version.toString)
      .build()
    HttpResponse.of(headers)
  }

  @Get("/shares/{share}/schemas/{schema}/tables")
  @ProducesJson
  def listTables(
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String): ListTablesResponse = {
    val (tables, nextPageToken) = shareManagement.listTables(share, schema, Option(pageToken), Some(maxResults))
    ListTablesResponse(tables, nextPageToken)
  }

  @Get("/shares/{share}/schemas/{schema}/tables/{table}/metadata")
  def getMetadata(
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("table") table: String): HttpResponse = {
    import scala.collection.JavaConverters._
    val tableConfig = shareManagement.getTable(share, schema, table)
    val (version, s) = DeltaTableHelper.query(
      tableConfig,
      false,
      Nil,
      None,
      serverConfig.preSignedUrlTimeoutSeconds)
    val headers = ResponseHeaders.builder(200)
      .set(HttpHeaderNames.CONTENT_TYPE, DELTA_TABLE_METADATA_CONTENT_TYPE)
      .set(DELTA_TABLE_VERSION_HEADER, version.toString)
      .build()
    com.linecorp.armeria.internal.server.ResponseConversionUtil.streamingFrom(
      s.asJava.stream(),
      headers,
      HttpHeaders.of(),
      (o: Any) => {
        val out = new ByteArrayOutputStream
        JsonUtils.toJson(out, o)
        out.write('\n')
        HttpData.wrap(out.toByteArray)
      },
      ServiceRequestContext.current().blockingTaskExecutor())
  }

  @Post("/shares/{share}/schemas/{schema}/tables/{table}/query")
  @ConsumesJson
  def listFiles(
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("table") table: String,
      queryTableRequest: QueryTableRequest): HttpResponse = {
    import scala.collection.JavaConverters._
    val tableConfig = shareManagement.getTable(share, schema, table)
    val (version, s) = DeltaTableHelper.query(
      tableConfig,
      true,
      queryTableRequest.predicateHints,
      queryTableRequest.limitHint,
      serverConfig.preSignedUrlTimeoutSeconds)
    val headers = ResponseHeaders.builder(200)
      .set(HttpHeaderNames.CONTENT_TYPE, DELTA_TABLE_METADATA_CONTENT_TYPE)
      .set(DELTA_TABLE_VERSION_HEADER, version.toString)
      .build()
    com.linecorp.armeria.internal.server.ResponseConversionUtil.streamingFrom(
      s.asJava.stream(),
      headers,
      HttpHeaders.of(),
      (o: Any) => {
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

  def start(serverConfig: ServerConfig): Server = {
    lazy val server = {
      import com.linecorp.armeria.server.Server
      val m = Class.forName("com.linecorp.armeria.server.scalapb.ScalaPbConverterUtil$").getDeclaredField("MODULE$").get(null)
      val f = Class.forName("com.linecorp.armeria.server.scalapb.ScalaPbConverterUtil$").getDeclaredField("defaultJsonPrinter")
      f.setAccessible(true)
      f.set(m, new Printer())

      val builder = Server.builder()
        .defaultHostname(serverConfig.getHost)
        .https(serverConfig.getPort)
        // TODO TLS Config
        .tlsSelfSigned()
        .disableDateHeader()
        .disableServerHeader()
        .annotatedService(serverConfig.endpoint, new DeltaSharingService(serverConfig): Any)
      if (serverConfig.getAuthorization != null) {
        // Authorization is set. Set up the authorization using the token in the server config.
        val authServiceBuilder =
          AuthService.builder.addOAuth2((_: ServiceRequestContext, token: OAuth2Token) => {
            // Use `MessageDigest.isEqual` to do a time-constant comparison to avoid timing attacks
            val authorized = MessageDigest.isEqual(
              token.accessToken.getBytes(UTF_8),
              serverConfig.getAuthorization.getBearerToken.getBytes(UTF_8))
            if (authorized) {
              CompletableFuture.completedFuture(true)
            } else {
              CompletableFuture.completedFuture(false)
            }
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
