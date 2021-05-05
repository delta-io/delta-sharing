package io.delta.exchange.server.config

import java.io.File
import java.util.Collections

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import scala.beans.BeanProperty

case class ServerConfig(
  @BeanProperty var version: Int,
  @BeanProperty var shares: java.util.List[ShareConfig],
  @BeanProperty var authorization: Authorization,
  @BeanProperty var host: String = "localhost",
  @BeanProperty var port: Int = 443,
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
}

object ServerConfig{
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

  def load(configFile: String): ServerConfig = {
    if (configFile.endsWith(".yaml") || configFile.endsWith(".yml")) {
      val serverConfig = createYamlObjectMapper.readValue(new File(configFile), classOf[ServerConfig])
      serverConfig.checkVersion()
      serverConfig
    }
    else {
      val serverConfig = createJsonObjectMapper.readValue(new File(configFile), classOf[ServerConfig])
      serverConfig.checkVersion()
      serverConfig
    }
  }

  def save(configFile: String, config: ServerConfig): Unit = {
    if (configFile.endsWith(".yaml") || configFile.endsWith(".yml")) {
      createYamlObjectMapper.writeValue(new File(configFile), config)
    } else {
      createJsonObjectMapper.writeValue(new File(configFile), config)
    }
  }
}
