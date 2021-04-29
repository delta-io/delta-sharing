package io.delta.exchange.server

import java.io.{File, PrintWriter}
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import com.amazonaws.auth.BasicAWSCredentials
import io.delta.exchange.protocol.Table
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
    val writer = new PrintWriter(testConfigFile)
    val shares = Seq(
      ShareConfig().withName("share1").withSchemas(
        Seq(
          SchemaConfig().withName("default").withTables(
            Seq(
              TableConfig().withName("table1")
                .withLocation(s"s3a://${TestResource.AWS.bucket}/delta-exchange-test/table1")
                .withConfiguration("test-config"),
              TableConfig().withName("table3")
                .withLocation(s"s3a://${TestResource.AWS.bucket}/delta-exchange-test/table3")
                .withConfiguration("test-config")
            )
          )
        )
      ).withRecipients(Seq(
        "recipient1",
        "recipient2"
      )),
      ShareConfig().withName("share2").withSchemas(
        Seq(
          SchemaConfig().withName("default").withTables(
            Seq(
              TableConfig().withName("table2")
                .withLocation(s"s3a://${TestResource.AWS.bucket}/delta-exchange-test/table2")
                .withConfiguration("test-config")
            )
          )
        )
      ).withRecipients(Seq(
        "recipient1"
      ))
    )
    val recipients = Seq(
      Recipient().withName("recipient1").withBearerToken(fakeTokenForRecipient1),
      Recipient().withName("recipient2").withBearerToken(fakeTokenForRecipient2)
    )
    val configurations = Seq(
      HadoopConfiguration().withName("test-config").withEntries(Seq(
        HadoopConfigurationEntry().withKey("fs.s3a.access.key").withValue(TestResource.AWS.awsAccessKey),
        HadoopConfigurationEntry().withKey("fs.s3a.secret.key").withValue(TestResource.AWS.awsSecretKey)
      ))
    )
    val serverConfig = ServerConfig()
      .withShares(shares)
      .withRecipients(recipients)
      .withConfigurations(configurations)
    writer.write(JsonFormat.toJsonString(serverConfig))
    writer.close()
    testConfigFile
  }
}
