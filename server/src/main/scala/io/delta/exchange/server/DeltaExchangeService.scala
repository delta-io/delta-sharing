package io.delta.exchange.server

import io.delta.standalone.internal.DeltaTableHelper
import io.delta.exchange.protocol.{GetFilesRequest, GetFilesResponse, GetMetadataRequest, GetMetadataResponse, GetTableInfoRequest, GetTableInfoResponse}

class DeltaExchangeService {

  import com.linecorp.armeria.server.annotation.{ConsumesJson, ProducesJson, Get}

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
