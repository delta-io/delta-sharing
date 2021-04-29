package io.delta.exchange.spark

import java.net.URI

import io.delta.exchange.client._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.JsonUtils

/** A test class that signs urls locally */
class DeltaLogLocalClient(tablePath: Path, signer: CloudFileSigner) extends DeltaExchangeClient {

  val deltaLog = DeltaLog.forTable(SparkSession.active, tablePath)

  override def getTableInfo(request: GetTableInfoRequest): GetTableInfoResponse = {
    GetTableInfoResponse(deltaLog.dataPath.toString.replace("file", "delta"), deltaLog.snapshot.version)
  }

  override def getMetadata(request: GetMetadataRequest): GetMetadataResponse = {
    GetMetadataResponse(JsonUtils.toJson(deltaLog.getSnapshotAt(request.version).metadata))
  }

  override def getFiles(request: GetFilesRequest): GetFilesResponse = {
    val addFiles = deltaLog.getSnapshotAt(request.version).allFiles.collect()
    GetFilesResponse(addFiles.map(signAddFile).map(a => JsonUtils.toJson(a)))
  }

  protected def absolutePath(child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      new Path(deltaLog.dataPath, p)
    }
  }

  protected def signAddFile(addFile: AddFile): AddFile = {
    val uri = absolutePath(addFile.path).toUri
    val bucket = if (uri.getUserInfo == null || uri.getUserInfo.isEmpty) {
      uri.getHost
    } else {
      uri.getUserInfo
    }
    val objectKey = uri.getPath.substring(1)
    addFile.copy(signer.sign(bucket, objectKey).toURI.toString)
  }

  override def listShares(): ListSharesResponse = ???

  override def getShare(request: GetShareRequest): GetShareResponse = ???
}
