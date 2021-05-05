package io.delta.exchange.server

import java.io.{File, PrintWriter}
import java.nio.file.Files
import java.util
import java.util.concurrent.TimeUnit

import com.amazonaws.auth.BasicAWSCredentials
import io.delta.exchange.protocol.Table
import io.delta.exchange.server.config.{Authorization, SchemaConfig, ServerConfig, ShareConfig, TableConfig}
import scalapb.json4s.JsonFormat

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

  val fakeTokenForRecipient1 = "dapi5e3574ec767ca1548ae5bbed1a2dc04d"
  val fakeTokenForRecipient2 = "dapi4e0a5ee596e45c73931d16df478d5234"

  def setupTestTables(): File = {
    val testConfigFile = Files.createTempFile("delta-exchange", ".json").toFile
    testConfigFile.deleteOnExit()
    println("file: " + testConfigFile)
    val writer = new PrintWriter(testConfigFile)
    val shares =  util.Arrays.asList(
      new ShareConfig("share1",
        util.Arrays.asList(
          new SchemaConfig(
            "default",
            util.Arrays.asList(
              new TableConfig("table1", s"s3a://${TestResource.AWS.bucket}/delta-exchange-test/table1"),
              new TableConfig("table3", s"s3a://${TestResource.AWS.bucket}/delta-exchange-test/table3")
            )
          )
        )
      ),
      new ShareConfig("share2",
        util.Arrays.asList(
          new SchemaConfig("default", util.Arrays.asList(
            new TableConfig("table2", s"s3a://${TestResource.AWS.bucket}/delta-exchange-test/table2")
          )
          )
        ))
    )
    val serverConfig = new ServerConfig(1, shares, new Authorization(fakeTokenForRecipient1))
    ServerConfig.save(testConfigFile.getCanonicalPath, serverConfig)
    testConfigFile
  }
}
