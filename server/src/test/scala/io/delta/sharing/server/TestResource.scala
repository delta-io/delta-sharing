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

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.io.FileUtils

import io.delta.sharing.server.config._

object TestResource {
  private val mapper = new ObjectMapper()

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

  /**
   * Builds a reference-server config from the shared catalog at
   * `dev/integration-tables.json` (also used by opensharing-test-server).
   */
  def setupTestTables(): File = {
    val testConfigFile = Files.createTempFile("delta-sharing", ".yaml").toFile
    testConfigFile.deleteOnExit()
    maybeSetupGoogleServiceAccountCredentials

    val shares = loadSharesFromClasspath().asJava

    val serverConfig = new ServerConfig()
    serverConfig.setVersion(1)
    serverConfig.setShares(shares)
    serverConfig.setAuthorization(Authorization(testAuthorizationToken))
    serverConfig.setPort(TEST_PORT)
    serverConfig.setSsl(SSLConfig(selfSigned = true, null, null, null))
    serverConfig.setEvaluatePredicateHints(true)
    serverConfig.setEvaluateJsonPredicateHints(true)

    serverConfig.save(testConfigFile.getCanonicalPath)
    testConfigFile
  }

  private def loadSharesFromClasspath(): Seq[ShareConfig] = {
    val stream = getClass.getClassLoader.getResourceAsStream("integration-tables.json")
    if (stream == null) {
      throw new IllegalStateException(
        "missing classpath resource integration-tables.json " +
          "(expected from dev/ via Test / unmanagedResourceDirectories)")
    }
    try {
      val root = mapper.readTree(stream)
      root.get("shares").elements().asScala.map { shareNode =>
        val schemas = shareNode.get("schemas").elements().asScala.map { schemaNode =>
          val tables = schemaNode.get("tables").elements().asScala.map { tableNode =>
            TableConfig(
              tableNode.get("name").asText(),
              tableNode.get("location").asText(),
              Option(tableNode.get("id")).map(_.asText()).getOrElse(""),
              Option(tableNode.get("historyShared")).exists(_.asBoolean()),
              Option(tableNode.get("startVersion")).map(_.asLong()).getOrElse(0L)
            )
          }.toSeq
          SchemaConfig(schemaNode.get("name").asText(), tables.asJava)
        }.toSeq
        ShareConfig(shareNode.get("name").asText(), schemas.asJava)
      }.toSeq
    } finally {
      stream.close()
    }
  }
}
