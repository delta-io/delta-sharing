package io.delta.exchange.client

class MockClient extends DeltaExchangeClient {

  override def listShares(): ListSharesResponse = {
    ListSharesResponse(Seq("foo", "bar"))
  }

  override def getShare(request: GetShareRequest): GetShareResponse = {
    GetShareResponse(SharedTable("xyz", "bar", "foo") :: Nil)
  }

  override def getTableInfo(request: GetTableInfoRequest): GetTableInfoResponse = throw new UnsupportedOperationException

  override def getMetadata(request: GetMetadataRequest): GetMetadataResponse = throw new UnsupportedOperationException

  override def getFiles(request: GetFilesRequest): GetFilesResponse = throw new UnsupportedOperationException
}
