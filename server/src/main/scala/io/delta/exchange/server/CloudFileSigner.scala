package io.delta.exchange.server

import java.net.URL
import java.util.concurrent.TimeUnit
import java.util.Date

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.HttpMethod
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest

trait CloudFileSigner {
  def sign(bucket: String, objectKey: String): URL
}

class S3FileSigner(
    awsCredentials: AWSCredentials,
    duration: Long, timeUnit: TimeUnit) extends CloudFileSigner {

  private val s3Client = new AmazonS3Client(awsCredentials)

  override def sign(bucket: String, objectKey: String): URL = {
    val expiration = new Date(System.currentTimeMillis() + timeUnit.toMillis(duration))
    val request = new GeneratePresignedUrlRequest(bucket, objectKey)
      .withMethod(HttpMethod.GET)
      .withExpiration(expiration)
    s3Client.generatePresignedUrl(request)
  }
}
