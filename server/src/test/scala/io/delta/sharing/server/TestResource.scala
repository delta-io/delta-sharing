package io.delta.sharing.server

import java.io.{File, PrintWriter}
import java.nio.file.Files
import io.delta.sharing.server.config._

object TestResource {
  def env(key: String): String = {
    sys.env.getOrElse(key, throw new IllegalArgumentException(s"Cannot find $key in sys env"))
  }

  object AWS {
    val bucket = "delta-exchange-test"
  }

  val TEST_PORT = 12345

  val testAuthorizationToken = "dapi5e3574ec767ca1548ae5bbed1a2dc04d"

  def setupTestTables(): File = {
    val testConfigFile = Files.createTempFile("delta-sharing", ".yaml").toFile
    testConfigFile.deleteOnExit()
    val shares = java.util.Arrays.asList(
      ShareConfig("share1",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig("table1", s"s3a://${TestResource.AWS.bucket}/delta-exchange-test/table1"),
              TableConfig("table3", s"s3a://${TestResource.AWS.bucket}/delta-exchange-test/table3")
            )
          )
        )
      ),
      ShareConfig("share2",
        java.util.Arrays.asList(
          SchemaConfig("default", java.util.Arrays.asList(
            TableConfig("table2", s"s3a://${TestResource.AWS.bucket}/delta-exchange-test/table2")
          )
          )
        ))
    )
    val serverConfig = ServerConfig(1, shares, Authorization(testAuthorizationToken), port = TEST_PORT)
    serverConfig.save(testConfigFile.getCanonicalPath)
    testConfigFile
  }
}
