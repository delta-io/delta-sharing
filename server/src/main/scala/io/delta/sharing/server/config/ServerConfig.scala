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

import java.io.{File, IOException}
import java.util.Collections

import scala.beans.BeanProperty

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

/** A trait that requires to implement */
trait ConfigItem {
  /** Verify whether the config is valid */
  def checkConfig(): Unit
}

/**
 * The class for the server config yaml file. The yaml file will be loaded as this class.
 *
 * As `jackson-dataformat-yaml` only supports Java,  we need to use `@BeanProperty var` to generate
 * Java bean classes.
 */
case class ServerConfig(
    @BeanProperty var version: java.lang.Integer,
    @BeanProperty var shares: java.util.List[ShareConfig],
    @BeanProperty var authorization: Authorization,
    @BeanProperty var ssl: SSLConfig,
    @BeanProperty var host: String,
    @BeanProperty var port: Int,
    @BeanProperty var endpoint: String,
    // The timeout of S3 presigned url in seconds
    @BeanProperty var preSignedUrlTimeoutSeconds: Long,
    // How many tables to cache in the memory.
    @BeanProperty var deltaTableCacheSize: Int,
    // Whether we can accept working with a stale version of the table. This is useful when sharing
    // static tables that will never be changed.
    @BeanProperty var stalenessAcceptable: Boolean,
    // Whether to evaluate user provided `predicateHints`
    @BeanProperty var evaluatePredicateHints: Boolean,
    // Whether to evaluate user provided `jsonPredicateHints`
    @BeanProperty var evaluateJsonPredicateHints: Boolean,
    // Whether to evaluate user provided `jsonPredicateHints` with V2 evaluator.
    @BeanProperty var evaluateJsonPredicateHintsV2: Boolean,
    // The timeout of an incoming web request in seconds. Set to 0 for no timeout
    @BeanProperty var requestTimeoutSeconds: Long,
    // The maximum page size permitted by queryTable/queryTableChanges API.
    @BeanProperty var queryTablePageSizeLimit: Int,
    // The TTL of the page token generated in queryTable/queryTableChanges API (in milliseconds).
    @BeanProperty var queryTablePageTokenTtlMs: Int,
    // The TTL of the refresh token generated in queryTable API (in milliseconds).
    @BeanProperty var refreshTokenTtlMs: Int
) extends ConfigItem {
  import ServerConfig._

  def this() = {
    // Set default values here
    this(
      version = null,
      shares = Collections.emptyList(),
      authorization = null,
      ssl = null,
      host = "localhost",
      port = 80,
      endpoint = "/delta-sharing",
      preSignedUrlTimeoutSeconds = 3600,
      deltaTableCacheSize = 10,
      stalenessAcceptable = false,
      evaluatePredicateHints = false,
      evaluateJsonPredicateHints = true,
      evaluateJsonPredicateHintsV2 = true,
      requestTimeoutSeconds = 30,
      queryTablePageSizeLimit = 10000,
      queryTablePageTokenTtlMs = 259200000, // 3 days
      refreshTokenTtlMs = 3600000 // 1 hour
    )
  }

  private def checkVersion(): Unit = {
    if (version == null) {
      throw new IllegalArgumentException("'version' must be provided")
    }
    if (version <= 0) {
      throw new IllegalArgumentException("'version' must be greater than 0")
    }
    if (version > CURRENT) {
      throw new IllegalArgumentException(s"The 'version' in the server config is $version which " +
        s"is too new. The current release supports version $CURRENT and below. " +
        s"Please upgrade to a newer release.")
    }
  }

  def save(configFile: String): Unit = {
    ServerConfig.save(this, configFile)
  }

  override def checkConfig(): Unit = {
    checkVersion()
    shares.forEach(_.checkConfig())
    if (authorization != null) {
      authorization.checkConfig()
    }
    if (ssl != null) {
      ssl.checkConfig()
    }
  }
}

object ServerConfig{
  /** The version that we understand */
  private val CURRENT = 1

  private def createYamlObjectMapper = {
    new ObjectMapper(new YAMLFactory)
      .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  }

  /**
   * Load the configurations for the server from the config file. If the file name ends with
   * `.yaml` or `.yml`, load it using the YAML parser. Otherwise, throw an error.
   */
  def load(configFile: String): ServerConfig = {
    if (configFile.endsWith(".yaml") || configFile.endsWith(".yml")) {
      val serverConfig =
        createYamlObjectMapper.readValue(new File(configFile), classOf[ServerConfig])
      serverConfig.checkConfig()
      serverConfig
    } else {
      throw new IOException("The server config file must be a yml or yaml file")
    }
  }

  /**
   * Serialize the [[ServerConfig]] object to the config file. If the file name ends with `.yaml`
   * or `.yml`, save it as a YAML file. Otherwise, throw an error.
   */
  def save(config: ServerConfig, configFile: String): Unit = {
    if (configFile.endsWith(".yaml") || configFile.endsWith(".yml")) {
      createYamlObjectMapper.writeValue(new File(configFile), config)
    } else {
      throw new IOException("The server config file must be a yml or yaml file")
    }
  }
}

case class Authorization(@BeanProperty var bearerToken: String) extends ConfigItem {

  def this() {
    this(null)
  }

  override def checkConfig(): Unit = {
    if (bearerToken == null) {
      throw new IllegalArgumentException("'bearerToken' in 'authorization' must be provided")
    }
  }
}

case class SSLConfig(
    @BeanProperty var selfSigned: Boolean,
    // The file of the PEM-format certificate
    @BeanProperty var certificateFile: String,
    // The file of the certificate’s private key
    @BeanProperty var certificateKeyFile: String,
    // The file storing the password to access the above certificate’s private key if it's protected
    @BeanProperty var certificatePasswordFile: String) extends ConfigItem {

  def this() {
    this(selfSigned = false, null, null, null)
  }

  override def checkConfig(): Unit = {
    if (!selfSigned) {
      if (certificateFile == null) {
        throw new IllegalArgumentException("'certificateFile' in a SSL config must be provided")
      }
      if (certificateKeyFile == null) {
        throw new IllegalArgumentException("'certificateKeyFile' in a SSL config must be provided")
      }
    }
  }
}

case class ShareConfig(
    @BeanProperty var name: String,
    @BeanProperty var schemas: java.util.List[SchemaConfig]) extends ConfigItem {

  def this() {
    this(null, Collections.emptyList())
  }

  override def checkConfig(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException("'name' in a share must be provided")
    }
    schemas.forEach(_.checkConfig())
  }
}

case class SchemaConfig(
    @BeanProperty var name: String,
    @BeanProperty var tables: java.util.List[TableConfig]) extends ConfigItem {

  def this() {
    this(null, Collections.emptyList())
  }

  override def checkConfig(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException("'name' in a schema must be provided")
    }
    tables.forEach(_.checkConfig())
  }
}

case class TableConfig(
    @BeanProperty var name: String,
    @BeanProperty var location: String,
    @BeanProperty var id: String = "",
    @BeanProperty var historyShared: Boolean = false,
    @BeanProperty var startVersion: Long = 0) extends ConfigItem {

  def this() {
    this(null, null, null)
  }

  override def checkConfig(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException("'name' in a table must be provided")
    }
    if (location == null) {
      throw new IllegalArgumentException("'location' in a table must be provided")
    }
  }
}
