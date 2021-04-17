package io.delta.exchange.server

import com.linecorp.armeria.common.{HttpRequest, HttpResponse, HttpStatus, MediaType}
import com.linecorp.armeria.server.ServiceRequestContext
import com.linecorp.armeria.server.annotation.{ExceptionHandler, ExceptionHandlerFunction}
import io.delta.standalone.internal.DeltaTableHelper
import io.delta.exchange.protocol._
import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.LoggerFactory

class DeltaExchangeServiceExceptionHandler extends ExceptionHandlerFunction {
  private val logger = LoggerFactory.getLogger(classOf[DeltaExchangeServiceExceptionHandler])

  override def handleException(
      ctx: ServiceRequestContext,
      req: HttpRequest,
      cause: Throwable): HttpResponse = {
    logger.error(cause.getMessage, cause)
    // TODO Dump error for debugging. Remove this before release.
    val error = ExceptionUtils.getFullStackTrace(cause)
    HttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE, MediaType.PLAIN_TEXT_UTF_8, error)
  }
}

@ExceptionHandler(classOf[DeltaExchangeServiceExceptionHandler])
class DeltaExchangeService {

  import com.linecorp.armeria.server.annotation.{ConsumesJson, ProducesJson, Get}

  private val shares = new FileBasedShareManagement()

  // curl -k -XGET -H 'content-type: application/json; charset=utf-8' 'https://localhost/api/2.0/s3commit/table/info' --data '{"uuid":"17d3543c-1a6f-480b-9f1b-22ef64a711b0"}'
  @Get("/info")
  @ConsumesJson
  @ProducesJson
  def getInfo(request: GetTableInfoRequest): GetTableInfoResponse = {
    DeltaTableHelper.getTableInfo(request)
  }

  // curl -k -XGET -H 'content-type: application/json; charset=utf-8' 'https://localhost/api/2.0/s3commit/table/metadata' --data '{"uuid":"17d3543c-1a6f-480b-9f1b-22ef64a711b0", "version": 0}'
  @Get("/metadata")
  @ConsumesJson
  @ProducesJson
  def getMetadata(request: GetMetadataRequest): GetMetadataResponse = {
    DeltaTableHelper.getMetadata(request)
  }

  // curl -k -XGET -H 'content-type: application/json; charset=utf-8' 'https://localhost/api/2.0/s3commit/table/files' --data '{"uuid":"17d3543c-1a6f-480b-9f1b-22ef64a711b0", "version": 0}'
  @Get("/files")
  @ConsumesJson
  @ProducesJson
  def getFiles(request: GetFilesRequest): GetFilesResponse = {
    DeltaTableHelper.getFiles(request)
  }

  // curl -k -XGET -H 'content-type: application/json; charset=utf-8' 'https://localhost/api/2.0/s3commit/table/shares'
  @Get("/shares")
  @ConsumesJson
  @ProducesJson
  def listShares(): ListSharesResponse = {
    ListSharesResponse().withName(shares.listShares())
  }

  // curl -k -XGET -H 'content-type: application/json; charset=utf-8' 'https://localhost/api/2.0/s3commit/table/share' --data '{"name":"vaccine_share"}'
  @Get("/share")
  @ConsumesJson
  @ProducesJson
  def getShare(share: GetShare): GetShareResponse = {
    GetShareResponse().withTable(shares.getShare(share.getName))
  }
}

object DeltaExchangeService {

  def main(args: Array[String]): Unit = {
    import com.linecorp.armeria.server.Server
    import com.linecorp.armeria.server.docs.DocService

    val server = Server.builder()
      .defaultHostname("localhost")
      .https(443)
      .tlsSelfSigned()
      .annotatedService("/api/2.0/s3commit/table", new DeltaExchangeService: Any)
      .serviceUnder("/docs", new DocService())
      .build()
    server.start()
    server.blockUntilShutdown()
  }
}
