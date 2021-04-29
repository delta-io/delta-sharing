package io.delta.exchange.server

import java.io.File

import com.google.common.io.Files
import io.delta.exchange.protocol.{Schema, Share, Table}
import scalapb.json4s.JsonFormat
import java.nio.charset.StandardCharsets.UTF_8

class FileBasedShareManagement(configFile: String) {

  val (shares, configurations, recipients) = {
    val serverConfigContent = Files.asCharSource(new File(configFile), UTF_8).read()
    val serverConfig = JsonFormat.fromJsonString[ServerConfig](serverConfigContent)
    validateServerConfig(serverConfig)
    (
      serverConfig.shares.map(share => share.getName -> share).toMap,
      serverConfig.configurations.map(c => c.getName -> c).toMap,
      serverConfig.recipients.map(r => r.getName -> r.getBearerToken).toMap,
    )
  }

  private def validateServerConfig(serverConfig: ServerConfig): Unit = {
    // TODO
  }

  def checkAccessToShare(bearerToken: String, share: String): Unit = {
    val shareConfig = shares.getOrElse(share, throw new ForbiddenException)
    shareConfig.recipients.foreach { r =>
      val token = recipients.getOrElse(r, throw new ForbiddenException)
      if (bearerToken == token) {
        return
      }
    }
    throw new ForbiddenException
  }

  def listShares(bearerToken: String): Seq[Share] = {
    println(recipients)
    val (recipient, _) =
      recipients.find(_._2 == bearerToken).getOrElse(throw new ForbiddenException)
    println(recipient)
    shares.filter(_._2.recipients.exists(_ == recipient)).values.map { share =>
      Share().withName(share.getName)
    }.toSeq
  }

  def listSchemas(bearerToken: String, share: String): Seq[Schema] = {
    checkAccessToShare(bearerToken, share)
    val shareConfig = shares.getOrElse(share, throw new NoSuchElementException)
    shareConfig.schemas.map { schemaConfig =>
      Schema().withName(schemaConfig.getName).withShare(share)
    }
  }

  def listTables(bearerToken: String, share: String, schema: String): Seq[Table] = {
    checkAccessToShare(bearerToken, share)
    val shareConfig = shares.getOrElse(share, throw new NoSuchElementException)
    val schemaConfig = shareConfig.schemas.find(_.getName == schema)
      .getOrElse(throw new NoSuchElementException)
    schemaConfig.tables.map { tableConfig =>
      Table().withName(tableConfig.getName).withSchema(schema).withShare(share)
    }
  }

  def getTable(bearerToken: String, share: String, schema: String, table: String): (TableConfig, HadoopConfiguration) =  {
    checkAccessToShare(bearerToken, share)
    val tableKey = (share, schema, table)
    val shareConfig = shares.getOrElse(share, throw new NoSuchElementException)
    val schemaConfig = shareConfig.schemas.find(_.getName == schema)
      .getOrElse(throw new NoSuchElementException)
    val tableConfig = schemaConfig.tables.find(_.getName == table)
      .getOrElse(throw new NoSuchElementException)
    tableConfig -> configurations(tableConfig.getConfiguration)
  }
}
