/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.sharing.server

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.commons.io.FileUtils

import io.delta.sharing.server.config._

object TestResource {
  def env(key: String): String = {
    sys.env.getOrElse(key, throw new IllegalArgumentException(s"Cannot find $key in sys env"))
  }

  object AWS {
    val bucket = "delta-exchange-test"
  }

  object Azure {
    val accountName = "deltasharingtest"
    val container = "delta-sharing-test-container"
  }

  object GCP {
    val bucket = "delta-sharing-dev"
  }

  val TEST_PORT = 12345

  val testAuthorizationToken = "dapi5e3574ec767ca1548ae5bbed1a2dc04d"

  def maybeSetupGoogleServiceAccountCredentials: Unit = {
    // Only setup Google Service Account credentials when it is provided through env variable.
    if (sys.env.get("GOOGLE_SERVICE_ACCOUNT_KEY").exists(_.length > 0)
        && sys.env.get("GOOGLE_APPLICATION_CREDENTIALS").exists(_.length > 0)) {
      val serviceAccountKey = sys.env("GOOGLE_SERVICE_ACCOUNT_KEY")
      val credFilePath = new File(sys.env("GOOGLE_APPLICATION_CREDENTIALS"))
      credFilePath.deleteOnExit()
      FileUtils.writeStringToFile(credFilePath, serviceAccountKey, UTF_8, false)
    }
  }

  def setupTestTables(): File = {
    val testConfigFile = Files.createTempFile("delta-sharing", ".yaml").toFile
    testConfigFile.deleteOnExit()
    maybeSetupGoogleServiceAccountCredentials
    val shares = java.util.Arrays.asList(
      ShareConfig("share1",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig("table1", s"s3a://${AWS.bucket}/delta-exchange-test/table1"),
              TableConfig("table3", s"s3a://${AWS.bucket}/delta-exchange-test/table3"),
              TableConfig("table7", s"s3a://${AWS.bucket}/delta-exchange-test/table7")
            )
          )
        )
      ),
      ShareConfig("share2",
        java.util.Arrays.asList(
          SchemaConfig("default", java.util.Arrays.asList(
            TableConfig("table2", s"s3a://${AWS.bucket}/delta-exchange-test/table2")
          )
          )
        )),
      ShareConfig("share3",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig("table4", s"s3a://${AWS.bucket}/delta-exchange-test/table4"),
              TableConfig("table5", s"s3a://${AWS.bucket}/delta-exchange-test/table5")
            )
          )
        )
      ),
      ShareConfig("share4",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              // table made with spark.sql.parquet.compression.codec=gzip
              TableConfig("test_gzip", s"s3a://${AWS.bucket}/compress-test/table1")
            )
          )
        )
      ),
      ShareConfig("share5",
        java.util.Arrays.asList(
          SchemaConfig(
            "default", // empty schema
            java.util.Arrays.asList()
          )
        )
      ),
      ShareConfig("share6",
        java.util.Arrays.asList()
      ),
      ShareConfig("share7",
        java.util.Arrays.asList(
          SchemaConfig(
            "schema1",
            java.util.Arrays.asList(
              TableConfig("table8", s"s3a://${AWS.bucket}/delta-exchange-test/table8")
            )
          ),
          SchemaConfig(
            "schema2",
            java.util.Arrays.asList(
              TableConfig("table9", s"s3a://${AWS.bucket}/delta-exchange-test/table9")
            )
          )
        )
      ),
      // scalastyle:off maxLineLength
      ShareConfig("share_azure",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig("table_wasb", s"wasbs://${Azure.container}@${Azure.accountName}.blob.core.windows.net/delta-sharing-test/table1"),
              TableConfig("table_abfs", s"abfss://${Azure.container}@${Azure.accountName}.dfs.core.windows.net/delta-sharing-test/table1")
            )
          )
        )
      ),
      // scalastyle:on
      ShareConfig("share_gcp",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig("table_gcs", s"gs://${GCP.bucket}/delta-sharing-test/table1")
            )
          )
        )
      )
    )

    val serverConfig = new ServerConfig()
    serverConfig.setVersion(1)
    serverConfig.setShares(shares)
    serverConfig.setAuthorization(Authorization(testAuthorizationToken))
    serverConfig.setPort(TEST_PORT)
    serverConfig.setSsl(SSLConfig(selfSigned = true, null, null, null))
    serverConfig.setEvaluatePredicateHints(true)

    serverConfig.save(testConfigFile.getCanonicalPath)
    testConfigFile
  }
}
