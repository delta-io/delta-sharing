package io.delta.sharing.server

import java.net.{URI, URL}
import java.util.Date
import java.util.concurrent.TimeUnit.SECONDS

import com.amazonaws.HttpMethod
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory
import org.apache.hadoop.util.ReflectionUtils

trait CloudFileSigner {
  def sign(bucket: String, objectKey: String): URL
}

class S3FileSigner(
    name: URI,
    conf: Configuration,
    preSignedUrlTimeoutSeconds: Long) extends CloudFileSigner {

  private val s3Client = ReflectionUtils.newInstance(classOf[DefaultS3ClientFactory], conf)
    .createS3Client(name)

  override def sign(bucket: String, objectKey: String): URL = {
    val expiration = new Date(System.currentTimeMillis() + SECONDS.toMillis(preSignedUrlTimeoutSeconds))
    val request = new GeneratePresignedUrlRequest(bucket, objectKey)
      .withMethod(HttpMethod.GET)
      .withExpiration(expiration)
    s3Client.generatePresignedUrl(request)
  }
}
