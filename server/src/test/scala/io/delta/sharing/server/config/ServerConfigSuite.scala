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

package io.delta.sharing.server.config

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.Arrays

import org.apache.commons.io.FileUtils
import org.scalatest.FunSuite

class ServerConfigSuite extends FunSuite {

  def testConfig(content: String, serverConfig: ServerConfig): Unit = {
    val tempFile = Files.createTempFile("delta-sharing-server", ".yaml").toFile
    try {
      FileUtils.writeStringToFile(tempFile, content, UTF_8)
      val loaded = ServerConfig.load(tempFile.getCanonicalPath)
      assert(serverConfig == loaded)
    } finally {
      tempFile.delete()
    }
  }

  test("empty config") {
    val serverConfig = new ServerConfig()
    serverConfig.setVersion(1)
    testConfig("version: 1", serverConfig)
  }

  test("template") {
    val tempFile = Files.createTempFile("delta-sharing-server", ".yaml").toFile
    try {
      FileUtils.copyFile(
        new File("src/universal/conf/delta-sharing-server.yaml.template"),
        tempFile)
      val loaded = ServerConfig.load(tempFile.getCanonicalPath)
      val sharesInTemplate = Arrays.asList(
        ShareConfig("share1", Arrays.asList(
          SchemaConfig("schema1", Arrays.asList(
            TableConfig(
              "table1",
              "s3a://<bucket-name>/<the-table-path>",
              id = "00000000-0000-0000-0000-000000000000"
            ),
            TableConfig(
              "table2",
              "wasbs://<container-name>@<account-name}.blob.core.windows.net/<the-table-path>",
              id = "00000000-0000-0000-0000-000000000001"
            )
          ))
        )),
        ShareConfig("share2", Arrays.asList(
          SchemaConfig("schema2", Arrays.asList(
            TableConfig(
              "table3",
              "abfss://<container-name>@<account-name}.dfs.core.windows.net/<the-table-path>",
              id = "00000000-0000-0000-0000-000000000002",
              historyShared = true
            )
          ))
        )),
        ShareConfig("share3", Arrays.asList(
          SchemaConfig("schema3", Arrays.asList(
            TableConfig(
              "table4",
              "gs://<bucket-name>/<the-table-path>",
              id = "00000000-0000-0000-0000-000000000003"
            )
          ))
        )),
        ShareConfig("share4", Arrays.asList(
          SchemaConfig("schema4", Arrays.asList(
            TableConfig(
              "table5",
              "s3a://<bucket-name>/<the-table-path>",
              id = "00000000-0000-0000-0000-000000000004"
            )
          ))
        ))
      )
      val serverConfig = new ServerConfig()
      serverConfig.setVersion(1)
      serverConfig.setShares(sharesInTemplate)
      serverConfig.setPort(8080)
      assert(loaded == serverConfig)
    } finally {
      tempFile.delete()
    }
  }

  test("accept unknown fields") {
    val serverConfig = new ServerConfig()
    serverConfig.setVersion(1)
    testConfig(
      """version: 1
        |unknown: "test"
        |""".stripMargin, serverConfig)
  }

  test("authorization token") {
    val serverConfig = new ServerConfig()
    serverConfig.setVersion(1)
    serverConfig.setAuthorization(Authorization("<token>"))
    testConfig(
      """version: 1
        |authorization:
        |  bearerToken: <token>
        |""".stripMargin, serverConfig)
  }

  private def assertInvalidConfig(expectedErrorMessage: String)(func: => Unit): Unit = {
    assert(intercept[IllegalArgumentException] {
      func
    }.getMessage.contains(expectedErrorMessage))
  }

  test("invalid version") {
    assertInvalidConfig("'version' must be greater than 0") {
      testConfig("version: 0", null)
    }
  }

  test("future version") {
    assertInvalidConfig("The 'version' in the server config is 100 which is too new.") {
      testConfig("version: 100", null)
    }
  }

  test("invalid ssl") {
    assertInvalidConfig("'certificateFile' in a SSL config must be provided") {
      testConfig(
        """version: 1
          |ssl:
          |  selfSigned: false
          |""".stripMargin, null)
    }
  }

  test("Authorization") {
    assertInvalidConfig("'bearerToken' in 'authorization' must be provided") {
      new Authorization().checkConfig()
    }
  }

  test("SSLConfig") {
    assertInvalidConfig("'certificateFile' in a SSL config must be provided") {
      val s = new SSLConfig()
      assert(s.selfSigned == false)
      s.checkConfig()
    }
    assertInvalidConfig("'certificateKeyFile' in a SSL config must be provided") {
      val s = new SSLConfig()
      s.setCertificateFile("file")
      s.checkConfig()
    }
    val s = new SSLConfig()
    s.setSelfSigned(true)
    s.checkConfig()
  }

  test("ShareConfig") {
    assertInvalidConfig("'name' in a share must be provided") {
      new ShareConfig().checkConfig()
    }
    assertInvalidConfig("'name' in a schema must be provided") {
      val s = new ShareConfig()
      s.setName("name")
      s.setSchemas(Arrays.asList(new SchemaConfig()))
      s.checkConfig()
    }
  }

  test("SchemaConfig") {
    assertInvalidConfig("'name' in a schema must be provided") {
      new SchemaConfig().checkConfig()
    }
    assertInvalidConfig("'name' in a table must be provided") {
      val s = new SchemaConfig()
      s.setName("name")
      s.setTables(Arrays.asList(new TableConfig()))
      s.checkConfig()
    }
  }

  test("TableConfig") {
    assertInvalidConfig("'name' in a table must be provided") {
      new TableConfig().checkConfig()
    }
    assertInvalidConfig("'name' in a table must be provided") {
      val t = new TableConfig()
      t.setLocation("Location")
      t.checkConfig()
    }
    assertInvalidConfig("'location' in a table must be provided") {
      val t = new TableConfig()
      t.setName("name")
      t.checkConfig()
    }
  }
}
