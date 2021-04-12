package io.delta.exchange.spark

import java.util.Base64
import java.util.concurrent.TimeUnit

import com.amazonaws.auth.BasicAWSCredentials

object TestResource {
  def env(key: String): String = {
    sys.env.getOrElse(key, throw new IllegalArgumentException(s"Cannot find $key in sys env"))
  }

  object AWS {
    val awsAccessKey = env("DELTA_EXCHANGE_TEST_AWS_ACCESS_KEY")
    val awsSecretKey = env("DELTA_EXCHANGE_TEST_AWS_SECRET_KEY")
    val bucket = env("DELTA_EXCHANGE_TEST_AWS_BUCKET")
    lazy val signer =
      new S3FileSigner(new BasicAWSCredentials(awsAccessKey, awsSecretKey), 15, TimeUnit.MINUTES)
  }

  object Azure {
    val storageAccount = env("DELTA_EXCHANGE_TEST_AZURE_STORAGE_ACCOUNT")
    val encodedCredential = env("DELTA_EXCHANGE_TEST_AZURE_ENCODED_CREDENTIAL")
    val credential = new String(Base64.getDecoder().decode(encodedCredential.getBytes()))
    lazy val signer = new AzureFileSigner(storageAccount, credential, 15, TimeUnit.MINUTES)
  }

  object GCP {
    val key = env("DELTA_EXCHANGE_TEST_GCP_KEY")
    val projectId = env("DELTA_EXCHANGE_TEST_GCP_PROJECT_ID")
    val bucket = env("DELTA_EXCHANGE_TEST_GCP_BUCKET")
    val clientEmail = env("DELTA_EXCHANGE_TEST_GCP_CLIENT_EMAIL")
    val keyId = env("DELTA_EXCHANGE_TEST_GCP_KEY_ID")
    lazy val signer = new GCSFileSigner(
      clientEmail,
      key,
      keyId,
      projectId,
      15,
      TimeUnit.MINUTES)
  }
}
