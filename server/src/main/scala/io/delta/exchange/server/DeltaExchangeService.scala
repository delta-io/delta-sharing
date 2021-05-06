package io.delta.exchange.server

import java.io.ByteArrayOutputStream
import java.{lang, util}
import java.util.{Collections, Optional}
import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.linecorp.armeria.server.auth.AuthService
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.linecorp.armeria.common.auth.OAuth2Token
import com.linecorp.armeria.common.{HttpData, HttpHeaderNames, HttpHeaders, HttpRequest, HttpResponse, HttpStatus, MediaType, ResponseHeaders}
import com.linecorp.armeria.server.ServiceRequestContext
import com.linecorp.armeria.server.annotation.{ConsumesJson, Default, ExceptionHandler, ExceptionHandlerFunction, Head, Header, JacksonResponseConverterFunction, Param, Post, ProducesJsonSequences}
import com.linecorp.armeria.server.auth.Authorizer
import io.delta.standalone.internal.DeltaTableHelper
import io.delta.exchange.protocol._
import io.delta.exchange.server.config.ServerConfig
import javax.annotation.Nullable
import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.LoggerFactory
import scalapb.json4s.Printer

object JsonUtils {
  /** Used to convert between classes and JSON. */
  lazy val mapper = {
    val _mapper = new ObjectMapper with ScalaObjectMapper
    _mapper.setSerializationInclusion(Include.NON_ABSENT)
    _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _mapper.registerModule(DefaultScalaModule)
    _mapper
  }

  def toJson[T: Manifest](obj: T): String = {
    mapper.writeValueAsString(obj)
  }

  def toPrettyJson[T: Manifest](obj: T): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
  }

  def fromJson[T: Manifest](json: String): T = {
    mapper.readValue[T](json)
  }
}


class DeltaExchangeServiceExceptionHandler extends ExceptionHandlerFunction {
  private val logger = LoggerFactory.getLogger(classOf[DeltaExchangeServiceExceptionHandler])

  override def handleException(
      ctx: ServiceRequestContext,
      req: HttpRequest,
      cause: Throwable): HttpResponse = {
    if (cause.isInstanceOf[NoSuchElementException]) {
      return HttpResponse.of(HttpStatus.NOT_FOUND)
    }
    logger.error(cause.getMessage, cause)
    // TODO Dump error for debugging. Remove this before release.
    val error = ExceptionUtils.getFullStackTrace(cause)
    HttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE, MediaType.PLAIN_TEXT_UTF_8, error)
  }
}

@ExceptionHandler(classOf[DeltaExchangeServiceExceptionHandler])
class DeltaExchangeService(serverConfig: ServerConfig) {

  import com.linecorp.armeria.server.annotation.{ProducesJson, Get}

  private val shareManagement = new ShareManagement(serverConfig)

  // curl -k -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares 2>/dev/null | json_pp
  @Get("/shares")
  @ProducesJson
  def listShares(
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String): ListSharesResponse = {
    val (shares, nextPageToken) = shareManagement.listShares(Option(pageToken), Some(maxResults))
    ListSharesResponse(shares, nextPageToken)
  }

  // curl -k -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share1/schemas 2>/dev/null | json_pp
  @Get("/shares/{share}/schemas")
  @ProducesJson
  def listSchemas(
      @Param("share") share: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String): ListSchemasResponse = {
    val (schemas, nextPageToken) = shareManagement.listSchemas(share, Option(pageToken), Some(maxResults))
    ListSchemasResponse(schemas, nextPageToken)
  }

  // curl -k -I -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share1/schemas/default/tables/table1
  @Head("/shares/{share}/schemas/{schema}/tables/{table}")
  def getTableVersion(
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("table") table: String): HttpResponse = {
    val tableConfig = shareManagement.getTable(share, schema, table)
    val version = DeltaTableHelper.getTableVersion(tableConfig)
    val headers = ResponseHeaders.builder(200)
      .set(DeltaExchangeService.DELTA_TABLE_VERSION_HEADER, version.toString)
      .build()
    HttpResponse.of(headers)
  }

  // curl -k -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share1/schemas/default/tables 2>/dev/null | json_pp
  @Get("/shares/{share}/schemas/{schema}/tables")
  @ProducesJson
  def listTables(
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Nullable pageToken: String): ListTablesResponse = {
    println("maxResults: " + maxResults)
    val (tables, nextPageToken) = shareManagement.listTables(share, schema, Option(pageToken), Some(maxResults))
    ListTablesResponse(tables, nextPageToken)
  }

  // table1 (non partitioned):
  // curl -k -i -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share1/schemas/default/tables/table1/metadata
  // table2 (partitioned):
  // curl -k -i -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share2/schemas/default/tables/table2/metadata
  // table3 (partitioned, data files have different schemas):
  // curl -k -i -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share1/schemas/default/tables/table3/metadata
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
      None)
    val headers = ResponseHeaders.builder(200)
      .set(HttpHeaderNames.CONTENT_TYPE, "application/x-ndjson; charset=utf-8")
      .set(DeltaExchangeService.DELTA_TABLE_VERSION_HEADER, version.toString)
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

  // table1 (non partitioned):
  // curl -k -i -XPOST -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" -H "Content-Type: application/json; charset=utf-8" --data "{}" https://localhost/delta-exchange/shares/share1/schemas/default/tables/table1/query
  // table2 (partitioned):
  // curl -k -i -XPOST -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" -H "Content-Type: application/json; charset=utf-8" --data "{}" https://localhost/delta-exchange/shares/share2/schemas/default/tables/table2/query
  // table3 (partitioned, data files have different schemas):
  // curl -k -i -XPOST -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" -H "Content-Type: application/json; charset=utf-8" --data "{}" https://localhost/delta-exchange/shares/share1/schemas/default/tables/table3/query
  @Post("/shares/{share}/schemas/{schema}/tables/{table}/query")
  @ConsumesJson
  def listFiles(
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("table") table: String,
      queryTableRequest: QueryTableRequest): HttpResponse = {
    import scala.collection.JavaConverters._
    println("predicates: " + queryTableRequest.predicateHints)
    val tableConfig = shareManagement.getTable(share, schema, table)
    val (version, s) = DeltaTableHelper.query(
      tableConfig,
      true,
      queryTableRequest.predicateHints,
      queryTableRequest.limitHint)
    val headers = ResponseHeaders.builder(200)
      .set(HttpHeaderNames.CONTENT_TYPE, "application/x-ndjson; charset=utf-8")
      .set(DeltaExchangeService.DELTA_TABLE_VERSION_HEADER, version.toString)
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

object DeltaExchangeService {
  val DELTA_TABLE_VERSION_HEADER = "Delta-Table-Version"

  // TODO Make these configurable
  private val endpoint = "/delta-exchange"
  var configFile =
    sys.env.getOrElse("DELTA_EXCHANGE_SERVER_CONFIG", TestResource.setupTestTables().getCanonicalPath)
  private val serverConfig = ServerConfig.load(configFile)

  lazy val server = {
    import com.linecorp.armeria.server.Server
    val m = Class.forName("com.linecorp.armeria.server.scalapb.ScalaPbConverterUtil$").getDeclaredField("MODULE$").get(null)
    val f = Class.forName("com.linecorp.armeria.server.scalapb.ScalaPbConverterUtil$").getDeclaredField("defaultJsonPrinter")
    f.setAccessible(true)
    f.set(m, new Printer())

    val builder = Server.builder()
      .defaultHostname(serverConfig.getHost)
      .https(serverConfig.getPort)
      .tlsSelfSigned()
      .disableDateHeader()
      .disableServerHeader()
      .annotatedService(endpoint, new DeltaExchangeService(serverConfig): Any)
    if (serverConfig.getAuthorization != null) {
      val authServiceBuilder = AuthService.builder
      authServiceBuilder.addOAuth2(new Authorizer[OAuth2Token]() {
        override def authorize(ctx: ServiceRequestContext, token: OAuth2Token): CompletionStage[lang.Boolean] = {
          val authorized = token.accessToken == serverConfig.getAuthorization.getBearerToken
          if (authorized) {
            CompletableFuture.completedFuture(true);
          } else {
            CompletableFuture.completedFuture(false);
          }
        }
      })
      builder.decorator(authServiceBuilder.newDecorator)
    }
    builder.build()
  }

  def requestPath(path: String) = {
    s"https://${serverConfig.getHost}:${serverConfig.getPort}$endpoint$path"
  }

  def start(): CompletableFuture[Void] = {
    server.start()
  }

  def stop(): CompletableFuture[Void] = {
    server.stop()
  }

  def main(args: Array[String]): Unit = {
    server.start().get()
    server.blockUntilShutdown()
  }
}
