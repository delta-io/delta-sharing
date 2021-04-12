package io.delta.exchange.server

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
}
