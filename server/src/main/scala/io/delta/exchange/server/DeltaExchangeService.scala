package io.delta.exchange.server

import java.io.ByteArrayOutputStream
import java.util
import java.util.{Collections, Optional}
import java.util.concurrent.CompletableFuture

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.collect.ImmutableListMultimap
import com.linecorp.armeria.common.{HttpData, HttpHeaderNames, HttpHeaders, HttpRequest, HttpResponse, HttpStatus, MediaType, ResponseHeaders}
import com.linecorp.armeria.server.ServiceRequestContext
import com.linecorp.armeria.server.annotation.{Default, ExceptionHandler, ExceptionHandlerFunction, Head, Header, JacksonResponseConverterFunction, Param, ProducesJsonSequences}
import com.linecorp.armeria.server.scalapb.ScalaPbResponseConverterFunction
import com.linecorp.armeria.server.streaming.JsonTextSequences
import io.delta.standalone.internal.DeltaTableHelper
import io.delta.exchange.protocol._
import io.delta.standalone.internal.actions.Metadata
import javax.annotation.Nullable
import org.apache.commons.lang.exception.ExceptionUtils
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import scalapb.json4s.Printer

class ForbiddenException extends RuntimeException

object Helper {
  lazy val mapper = {
    val _mapper = new ObjectMapper with ScalaObjectMapper
    _mapper.enable(SerializationFeature.INDENT_OUTPUT)
    _mapper.setSerializationInclusion(Include.NON_ABSENT)
    _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _mapper.registerModule(DefaultScalaModule)
    _mapper
  }
}

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
    if (cause.isInstanceOf[ForbiddenException]) {
      cause.printStackTrace()
      return HttpResponse.of(HttpStatus.FORBIDDEN)
    }
    logger.error(cause.getMessage, cause)
    // TODO Dump error for debugging. Remove this before release.
    val error = ExceptionUtils.getFullStackTrace(cause)
    HttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE, MediaType.PLAIN_TEXT_UTF_8, error)
  }
}

@ExceptionHandler(classOf[DeltaExchangeServiceExceptionHandler])
class DeltaExchangeService(configFile: String) {

  import com.linecorp.armeria.server.annotation.{ProducesJson, Get}

  private val shareManagement = new FileBasedShareManagement(configFile)

  def getBearerToken(auth: String): String = {
    if (auth.startsWith("Bearer ")) {
      auth.stripPrefix("Bearer ")
    } else {
      throw new ForbiddenException()
    }
  }

  // curl -k -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares 2>/dev/null | json_pp
  @Get("/shares")
  @ProducesJson
  def listShares(
      @Header("Authorization") auth: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Default("") pageToken: String): ListSharesResponse = {
    ListSharesResponse(shareManagement.listShares(getBearerToken(auth)))
  }

  // curl -k -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share1/schemas 2>/dev/null | json_pp
  @Get("/shares/{share}/schemas")
  @ProducesJson
  def listSchemas(
      @Header("Authorization") auth: String,
      @Param("share") share: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Default("") pageToken: String): ListSchemasResponse = {
    ListSchemasResponse(shareManagement.listSchemas(getBearerToken(auth), share))
  }

  // curl -k -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share1/schemas/default/tables 2>/dev/null | json_pp
  @Get("/shares/{share}/schemas/{schema}/tables")
  @ProducesJson
  def listTables(
     @Header("Authorization") auth: String,
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("maxResults") @Default("500") maxResults: Int,
      @Param("pageToken") @Default("") pageToken: String): ListTablesResponse = {
    ListTablesResponse(shareManagement.listTables(getBearerToken(auth), share, schema))
  }

  // table1 (non partitioned):
  // curl -k -i -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share1/schemas/default/tables/table1/metadata
  // table2 (partitioned):
  // curl -k -i -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share2/schemas/default/tables/table2/metadata
  // table3 (partitioned, data files have different schemas):
  // curl -k -i -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share1/schemas/default/tables/table3/metadata
  @Get("/shares/{share}/schemas/{schema}/tables/{table}/metadata")
  def getMetadata(
    @Header("Authorization") auth: String,
    @Param("share") share: String,
    @Param("schema") schema: String,
    @Param("table") table: String): HttpResponse = {
    val headers = ResponseHeaders.builder(200)
      .set(HttpHeaderNames.CONTENT_TYPE, "application/x-ndjson; charset=utf-8")
      .build()
    import scala.collection.JavaConverters._
    val (tableConfig, hadoopConfiguration) =
      shareManagement.getTable(getBearerToken(auth), share, schema, table)
    val s = DeltaTableHelper.query(
      tableConfig,
      hadoopConfiguration,
      false,
      Nil,
      Optional.empty()).asJava.stream()
    com.linecorp.armeria.internal.server.ResponseConversionUtil.streamingFrom(
      s,
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
  // curl -k -i -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share1/schemas/default/tables/table1/query
  // table2 (partitioned):
  // curl -k -i -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share2/schemas/default/tables/table2/query
  // table3 (partitioned, data files have different schemas):
  // curl -k -i -XGET -H "Authorization: Bearer dapi5e3574ec767ca1548ae5bbed1a2dc04d" https://localhost/delta-exchange/shares/share1/schemas/default/tables/table3/query
  @Get("/shares/{share}/schemas/{schema}/tables/{table}/query")
  def listFiles(
      @Header("Authorization") auth: String,
      @Param("share") share: String,
      @Param("schema") schema: String,
      @Param("table") table: String,
      @Nullable @Param("predicates") predicates: String,
      @Param("limit") limit: Optional[Int]): HttpResponse = {
    val headers = ResponseHeaders.builder(200)
      .set(HttpHeaderNames.CONTENT_TYPE, "application/x-ndjson; charset=utf-8")
      .build()
    import scala.collection.JavaConverters._
    val (tableConfig, hadoopConfiguration) =
      shareManagement.getTable(getBearerToken(auth), share, schema, table)
    val s = DeltaTableHelper.query(
      tableConfig,
      hadoopConfiguration,
      true,
      Option(predicates).map(JsonUtils.fromJson[Seq[String]]).getOrElse(Nil),
      limit).asJava.stream()
    com.linecorp.armeria.internal.server.ResponseConversionUtil.streamingFrom(
      s,
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

  // TODO Make these configurable
  private val host = "localhost"
  private val port = 443
  private val endpoint = "/delta-exchange"
  var configFile = TestResource.setupTestTables().getCanonicalPath

  lazy val server = {
    import com.linecorp.armeria.server.Server
    import com.linecorp.armeria.server.docs.DocService
    val m = Class.forName("com.linecorp.armeria.server.scalapb.ScalaPbConverterUtil$").getDeclaredField("MODULE$").get(null)
    val f = Class.forName("com.linecorp.armeria.server.scalapb.ScalaPbConverterUtil$").getDeclaredField("defaultJsonPrinter")
    f.setAccessible(true)
    f.set(m, new Printer())

    Server.builder()
      .defaultHostname(host)
      .https(port)
      .tlsSelfSigned()
      .annotatedService(endpoint, new DeltaExchangeService(configFile): Any)
      .serviceUnder("/docs", new DocService())
      .build()
  }

  def requestPath(path: String) = {
    s"https://$host:$port$endpoint$path"
  }

  def start(): CompletableFuture[Void] = {
    server.start()
  }

  def stop(): CompletableFuture[Void] = {
    server.stop()
  }

  def main(args: Array[String]): Unit = {
    server.start()
    server.blockUntilShutdown()
  }
}
