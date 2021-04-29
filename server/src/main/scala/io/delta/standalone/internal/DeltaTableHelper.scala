package io.delta.standalone.internal

import java.net.URI
import java.util.Optional
import java.util.concurrent.TimeUnit

import com.amazonaws.auth.BasicAWSCredentials
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import io.delta.exchange.protocol._
import io.delta.exchange.server.{CloudFileSigner, HadoopConfiguration, S3FileSigner, TableConfig}
import io.delta.standalone.internal.actions.{AddFile, SingleAction}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Expression, ExtractValue, Literal}
import org.apache.spark.sql.types.{DataType, MapType, StructField, StructType}

object DeltaTableHelper {
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

//  // TODO Cache DeltaLog
//  def getTableInfo(request: GetTableInfoRequest): GetTableInfoResponse = withClassLoader {
//    println("this: " + this.getClass.getClassLoader)
//    println("context: " + Thread.currentThread().getContextClassLoader)
//    val deltaLog = getDeltaLog(request.getUuid)
//    val response = GetTableInfoResponse()
//      .withPath(deltaLog.dataPath.toString)
//      .withVersion(deltaLog.snapshot.version)
//    response
//  }
//
//  def getMetadata(request: GetMetadataRequest): GetMetadataResponse = withClassLoader {
//    val deltaLog = getDeltaLog(request.getUuid)
//    val snapshot = deltaLog.getSnapshotForVersionAsOf(request.getVersion)
//    val stateMethod = snapshot.getClass.getMethod("state")
//    val state = stateMethod.invoke(snapshot).asInstanceOf[SnapshotImpl.State]
//    val metadata = state.metadata
//    val response = GetMetadataResponse().withMetadata(toJson(metadata))
//    response
//  }

//  def getFiles(request: GetFilesRequest): GetFilesResponse = withClassLoader {
//    val deltaLog = getDeltaLog(request.getUuid)
//    val snapshot = deltaLog.getSnapshotForVersionAsOf(request.getVersion)
//    val stateMethod = snapshot.getClass.getMethod("state")
//    val state = stateMethod.invoke(snapshot).asInstanceOf[SnapshotImpl.State]
//
//    println("request.partitionFilter: " + request.partitionFilter)
//
//    val selectedFiles = request.partitionFilter match {
//      case Some(f) =>
//        val schema = DataType.fromJson(state.metadata.schemaString).asInstanceOf[StructType]
//        val partitionSchema =
//          new StructType(state.metadata.partitionColumns.map(c => schema(c)).toArray)
//        ParserUtils.evaluatePredicate(partitionSchema, f, state.activeFiles.values.toSeq)
//      case None => state.activeFiles.values.toSeq
//    }
//
//    val response = GetFilesResponse().withFile(
//      selectedFiles.map { addFile =>
//        val s3Path = getS3Path(request.getUuid, addFile.path)
//        val signedUrl = signFile(s3Path)
//        println(s"path: $s3Path signed: $signedUrl")
//        toJson(addFile.copy(path = signFile(s3Path)))
//      }
//    )
//    response
//  }


  def query(tableConfig: TableConfig, hadoopConfiguration: HadoopConfiguration, shouldReturnFiles: Boolean, predicates: Seq[String], limit: Optional[Int]): Seq[Any] = withClassLoader {
    val deltaLog = getDeltaLog(tableConfig, hadoopConfiguration)
    val awsAccessKey =
      hadoopConfiguration.entries.find(_.getKey == "fs.s3a.access.key").head.getValue
    val awsSecretKey =
      hadoopConfiguration.entries.find(_.getKey == "fs.s3a.secret.key").head.getValue
    val snapshot = deltaLog.snapshot
    val stateMethod = snapshot.getClass.getMethod("state")
    val state = stateMethod.invoke(snapshot).asInstanceOf[SnapshotImpl.State]
    // TODO predicates, limit
    val selectedFiles = state.activeFiles.values.toSeq
    // TODO 15 should be a config
    val signer = new S3FileSigner(new BasicAWSCredentials(awsAccessKey, awsSecretKey), 15, TimeUnit.MINUTES)
    Seq(state.protocol.wrap, state.metadata.wrap) ++ {
      if (shouldReturnFiles) {
        selectedFiles.map { addFile =>
          val cloudPath = absolutePath(deltaLog.dataPath, addFile.path)
          val signedUrl = signFile(signer, cloudPath)
          println(s"path: $cloudPath signed: $signedUrl")
          addFile.copy(path = signedUrl).wrap
        }
      } else {
        Nil
      }
    }
  }

  private def absolutePath(path: Path, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      new Path(path, p)
    }
  }

  def getDeltaLog(tableConfig: TableConfig, hadoopConfiguration: HadoopConfiguration): DeltaLogImpl = {
    import io.delta.standalone.DeltaLog
    import org.apache.hadoop.conf.Configuration
    val conf = new Configuration()
    hadoopConfiguration.entries.foreach { c =>
      conf.set(c.getKey, c.getValue)
    }
    DeltaLog.forTable(conf, tableConfig.getLocation).asInstanceOf[DeltaLogImpl]
  }

  def signFile(signer: CloudFileSigner, path: Path): String = {
    val absPath = path.toUri
    val bucketName = absPath.getHost
    val objectKey = absPath.getPath.stripPrefix("/")
    signer.sign(bucketName, objectKey).toString
  }
}
