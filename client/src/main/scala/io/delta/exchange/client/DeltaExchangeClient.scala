package io.delta.exchange.client

case class GetTableInfoRequest(uuid: String)

case class GetTableInfoResponse(path: String, version: Long)

case class GetMetadataRequest(uuid: String, version: Long)

// metadata is the json format of Metadata
case class GetMetadataResponse(metadata: String)

case class GetFilesRequest(uuid: String, version: Long, partitionFilter: Option[String])

// file is the json format of AddFile whose path is a pre-signed url
case class GetFilesResponse(file: Seq[String])

case class ListSharesResponse(name: Seq[String])

case class GetShareRequest(name: String)

case class SharedTable(name: String, schema: String, shareName: String)

case class GetShareResponse(table: Seq[SharedTable])

trait DeltaExchangeClient {
  def getTableInfo(request: GetTableInfoRequest): GetTableInfoResponse

  def getMetadata(request: GetMetadataRequest): GetMetadataResponse

  def getFiles(request: GetFilesRequest): GetFilesResponse

  def listShares(): ListSharesResponse

  def getShare(request: GetShareRequest): GetShareResponse
}
