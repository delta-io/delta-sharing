package io.delta.sharing.server.config

import java.io.File
import java.util.Collections

import scala.beans.BeanProperty

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

case class Authorization(@BeanProperty var bearerToken: String) {
  def this() {
    this(null)
  }
}

case class ShareConfig(
  @BeanProperty var name: String,
  @BeanProperty var schemas: java.util.List[SchemaConfig]) {
  def this() {
    this(null, null)
  }
}

case class SchemaConfig(
  @BeanProperty var name: String,
  @BeanProperty var tables: java.util.List[TableConfig]) {
  def this() {
    this(null, null)
  }
}

case class TableConfig(@BeanProperty name: String, @BeanProperty var location: String) {

  def this() {
    this(null, null)
  }
}

case class ServerConfig(
  @BeanProperty var version: Int,
  @BeanProperty var shares: java.util.List[ShareConfig],
  @BeanProperty var authorization: Authorization,
  @BeanProperty var host: String = "localhost",
  @BeanProperty var port: Int = 443,
  @BeanProperty var endpoint: String = "/delta-sharing",
  @BeanProperty var preSignedUrlTimeoutSeconds: Long = 15 * 60
  // TODO TLS config
) {
  import ServerConfig._

  def this() = {
    this(1, Collections.emptyList(), null)
  }

  private def checkVersion(): Unit = {
    if (version > CURRENT) {
      throw new IllegalArgumentException(s"The 'version' ($version) in the server config is " +
        s"too new. The current release supports version $CURRENT and below. " +
        s"Please upgrade to a newer release.")
    }
  }

  def save(configFile: String): Unit = {
    ServerConfig.save(this, configFile)
  }
}

object ServerConfig{
  /** The version that we understand */
  private val CURRENT = 1

  private def createJsonObjectMapper = {
    new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT)
      .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  }

  private def createYamlObjectMapper = {
    new ObjectMapper(new YAMLFactory)
      .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  }

  /**
   * Load the configurations for the server from the config file. If the file name ends with
   * `.yaml` or `.yml`, load it using the YAML parser. Otherwise, use the JSON parser to load it.
   */
  def load(configFile: String): ServerConfig = {
    if (configFile.endsWith(".yaml") || configFile.endsWith(".yml")) {
      val serverConfig =
        createYamlObjectMapper.readValue(new File(configFile), classOf[ServerConfig])
      serverConfig.checkVersion()
      serverConfig
    } else {
      val serverConfig =
        createJsonObjectMapper.readValue(new File(configFile), classOf[ServerConfig])
      serverConfig.checkVersion()
      serverConfig
    }
  }

  /**
   * Serialize the [[ServerConfig]] object to the config file. If the file name ends with `.yaml`
   * or `.yml`, save it as a YAML file. Otherwise, save it as a JSON file.
   */
  def save(config: ServerConfig, configFile: String): Unit = {
    if (configFile.endsWith(".yaml") || configFile.endsWith(".yml")) {
      createYamlObjectMapper.writeValue(new File(configFile), config)
    } else {
      createJsonObjectMapper.writeValue(new File(configFile), config)
    }
  }
}
