package io.delta.exchange.server

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import io.delta.exchange.protocol.{PageToken, Schema, Share, Table}
import io.delta.exchange.server.config.{ServerConfig, TableConfig}

import scala.collection.JavaConverters._

class ShareManagement(serverConfig: ServerConfig) {

  val shares = serverConfig.getShares.asScala.map(share => share.getName -> share).toMap

  private def encodePageToken(id: String): String = {
    val binary = PageToken().withId(id.toString).toByteArray
    val x = new String(Base64.getUrlEncoder().encode(binary), UTF_8)
    println("encoded: " + x)
    x
  }

  def decodePageToken(pageToken: String): String = {
    println("to decode: " + pageToken)
    val binary = Base64.getUrlDecoder().decode(pageToken.getBytes(UTF_8))
    PageToken.parseFrom(binary).id.getOrElse {
      throw new IllegalArgumentException("invalid nextPageToken")
    }
  }

  private def getPage[T](
      nextPageToken: Option[String],
      maxResults: Option[Int],
      totalSize: Int)(func: (Int, Int) => Seq[T]): (Seq[T], Option[String]) = {
    val start = nextPageToken.map(pageToken => decodePageToken(pageToken).toInt).getOrElse(0)
    val end = start + maxResults.getOrElse(500)
    val results = func(start, end)
    val nextId = if (end < totalSize) Some(end) else None
    results -> nextId.map(id => encodePageToken(id.toString))
  }

  def listShares(nextPageToken: Option[String] = None, maxResults: Option[Int] = None): (Seq[Share], Option[String]) = {
    getPage(nextPageToken, maxResults, shares.size) { (start, end) =>
      shares.values.map { share =>
        Share().withName(share.getName)
      }.toSeq.slice(start, end)
    }
  }

  def listSchemas(share: String, nextPageToken: Option[String] = None, maxResults: Option[Int] = None): (Seq[Schema], Option[String]) = {
    val shareConfig = shares.getOrElse(share, throw new NoSuchElementException)
    getPage(nextPageToken, maxResults, shareConfig.getSchemas.size) { (start, end) =>
      shareConfig.getSchemas.asScala.map { schemaConfig =>
        Schema().withName(schemaConfig.getName).withShare(share)
      }.toSeq.slice(start, end)
    }
  }

  def listTables(share: String, schema: String, nextPageToken: Option[String] = None, maxResults: Option[Int] = None): (Seq[Table], Option[String]) = {
    val shareConfig = shares.getOrElse(share, throw new NoSuchElementException)
    val schemaConfig = shareConfig.getSchemas.asScala.find(_.getName == schema)
      .getOrElse(throw new NoSuchElementException)
    getPage(nextPageToken, maxResults, schemaConfig.getTables.size) { (start, end) =>
      schemaConfig.getTables.asScala.map { tableConfig =>
        Table().withName(tableConfig.getName).withSchema(schema).withShare(share)
      }.toSeq.slice(start, end)
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
