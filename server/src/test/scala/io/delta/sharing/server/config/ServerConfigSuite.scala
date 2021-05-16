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

import java.nio.file.Files
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays
import org.apache.commons.io.FileUtils
import org.scalatest.FunSuite

class ServerConfigSuite extends FunSuite {

  def testJsonConfig(serverConfig: ServerConfig): Unit = {
    val tempFile = Files.createTempFile("delta-sharing-server", ".json").toFile
    try {
      serverConfig.save(tempFile.getCanonicalPath)
      assert(serverConfig == ServerConfig.load(tempFile.getCanonicalPath))
    } finally {
      tempFile.delete()
    }
  }

  def testYamlConfig(serverConfig: ServerConfig): Unit = {
    val tempFile = Files.createTempFile("delta-sharing-server", ".yaml").toFile
    try {
      serverConfig.save(tempFile.getCanonicalPath)
      assert(serverConfig == ServerConfig.load(tempFile.getCanonicalPath))
    } finally {
      tempFile.delete()
    }
  }

  test("empty config") {
    val serverConfig = new ServerConfig(1, new java.util.ArrayList(), null)
    testJsonConfig(serverConfig)
    testYamlConfig(serverConfig)
  }

  test("authorization config") {
    val serverConfig = new ServerConfig(1, new java.util.ArrayList(), new Authorization("token"))
    testJsonConfig(serverConfig)
    testYamlConfig(serverConfig)
  }

  test("preSignedUrlTimeoutSeconds config") {
    val serverConfig = new ServerConfig(1, new java.util.ArrayList(), new Authorization("token"))
    serverConfig.setPreSignedUrlTimeoutSeconds(1000);
    testJsonConfig(serverConfig)
    testYamlConfig(serverConfig)
  }

  test("shares config") {
    val tables = Arrays.asList(
      new TableConfig("table1", "/foo/table1"),
      new TableConfig("table1", "/foo/table2")
    )
    val schemas = Arrays.asList(
      new SchemaConfig("default", tables)
    )
    val shares = Arrays.asList(
      new ShareConfig("share1", schemas),
      new ShareConfig("share2", schemas)
    )
    val serverConfig = new ServerConfig(1, shares, new Authorization("token"))
    testJsonConfig(serverConfig)
    testYamlConfig(serverConfig)
  }

  test("shares config without authorization") {
    val tables = Arrays.asList(
      new TableConfig("table1", "/foo/table1"),
      new TableConfig("table1", "/foo/table2")
    )
    val schemas = Arrays.asList(
      new SchemaConfig("default", tables)
    )
    val shares = Arrays.asList(
      new ShareConfig("share1", schemas),
      new ShareConfig("share2", schemas)
    )
    val serverConfig = new ServerConfig(1, shares, null)
    testJsonConfig(serverConfig)
    testYamlConfig(serverConfig)
  }
}
