package io.delta.standalone.internal

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import io.delta.exchange.protocol._

object DeltaTableHelper {
  import io.delta.exchange.server.TestResource.AWS._

  lazy val mapper = {
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.setSerializationInclusion(Include.NON_ABSENT)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    mapper
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

  def withClassLoader[T](func: => T): T = {
    val classLoader = Thread.currentThread().getContextClassLoader
    if (classLoader == null) {
      Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
      try {
        func
      } finally {
        Thread.currentThread().setContextClassLoader(null)
      }
    } else {
      func
    }
  }

  // TODO Cache DeltaLog
  def getTableInfo(request: GetTableInfoRequest): GetTableInfoResponse = withClassLoader {
    println("this: " + this.getClass.getClassLoader)
    println("context: " + Thread.currentThread().getContextClassLoader)
    val deltaLog = getDeltaLog(request.getUuid)
    val response = GetTableInfoResponse()
      .withPath(deltaLog.dataPath.toString)
      .withVersion(deltaLog.snapshot.version)
    response
  }

  def getMetadata(request: GetMetadataRequest): GetMetadataResponse = withClassLoader {
    val deltaLog = getDeltaLog(request.getUuid)
    val snapshot = deltaLog.getSnapshotForVersionAsOf(request.getVersion)
    val stateMethod = snapshot.getClass.getMethod("state")
    val state = stateMethod.invoke(snapshot).asInstanceOf[SnapshotImpl.State]
    val metadata = state.metadata
    val response = GetMetadataResponse().withMetadata(toJson(metadata))
    response
  }

  def getFiles(request: GetFilesRequest): GetFilesResponse = withClassLoader {
    val deltaLog = getDeltaLog(request.getUuid)
    val snapshot = deltaLog.getSnapshotForVersionAsOf(request.getVersion)
    val stateMethod = snapshot.getClass.getMethod("state")
    val state = stateMethod.invoke(snapshot).asInstanceOf[SnapshotImpl.State]
    val response = GetFilesResponse().withFile(
      state.activeFiles
        .mapValues { addFile =>
          val s3Path = getS3Path(request.getUuid, addFile.path)
          val signedUrl = signFile(s3Path)
          println(s"path: $s3Path signed: $signedUrl")
          toJson(addFile.copy(path = signFile(s3Path)))
        }
        .values
        .toSeq
    )
    response
  }

  def getDeltaLog(uuid: String): DeltaLogImpl = {
    import io.delta.standalone.DeltaLog
    import org.apache.hadoop.conf.Configuration
    val conf = new Configuration()
    conf.set("fs.s3a.access.key", awsAccessKey)
    conf.set("fs.s3a.secret.key", awsSecretKey)
    val path = s"s3a://$bucket/ryan_delta_remote_test/$uuid"
    DeltaLog.forTable(conf, path).asInstanceOf[DeltaLogImpl]
  }

  def getS3Path(uuid: String, relativePath: String): String = {
    s"s3a://$bucket/ryan_delta_remote_test/$uuid/$relativePath"
  }

  def signFile(s3Path: String): String = {
    val absPath = new java.net.URI(s3Path)
    val bucketName = absPath.getHost
    val objectKey = absPath.getPath.stripPrefix("/")
    signer.sign(bucketName, objectKey).toString
  }
}
