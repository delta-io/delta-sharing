package io.delta.exchange.server

import io.delta.exchange.protocol.{Schema, Share, Table}

import io.delta.exchange.server.config.{ServerConfig, TableConfig}

import scala.collection.JavaConverters._

class ShareManagement(serverConfig: ServerConfig) {

  val shares = serverConfig.getShares.asScala.map(share => share.getName -> share).toMap

  def listShares(): Seq[Share] = {
    shares.values.map { share =>
      Share().withName(share.getName)
    }.toSeq
  }

  def listSchemas(share: String): Seq[Schema] = {
    val shareConfig = shares.getOrElse(share, throw new NoSuchElementException)
    shareConfig.getSchemas.asScala.map { schemaConfig =>
      Schema().withName(schemaConfig.getName).withShare(share)
    }
  }

  def listTables(share: String, schema: String): Seq[Table] = {
    val shareConfig = shares.getOrElse(share, throw new NoSuchElementException)
    val schemaConfig = shareConfig.getSchemas.asScala.find(_.getName == schema)
      .getOrElse(throw new NoSuchElementException)
    schemaConfig.getTables.asScala.map { tableConfig =>
      Table().withName(tableConfig.getName).withSchema(schema).withShare(share)
    }
  }

  def getTable(share: String, schema: String, table: String): TableConfig =  {
    val shareConfig = shares.getOrElse(share, throw new NoSuchElementException)
    val schemaConfig = shareConfig.getSchemas.asScala.find(_.getName == schema)
      .getOrElse(throw new NoSuchElementException)
    val tableConfig = schemaConfig.getTables.asScala.find(_.getName == table)
      .getOrElse(throw new NoSuchElementException)
    tableConfig
  }
}
