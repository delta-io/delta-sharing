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

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import scala.collection.JavaConverters._

import io.delta.sharing.server.config.{SchemaConfig, ServerConfig, ShareConfig, TableConfig}
import io.delta.sharing.server.protocol.{PageToken, Schema, Share, Table}

/**
 * Load the shared tables from `ServerConfig` and provide the pagination APIs to query
 * shares/schemas/tables.
 */
class SharedTableManager(serverConfig: ServerConfig) {

  private val caseInsensitiveComparer = (a: String, b: String) => a.equalsIgnoreCase(b)

  private val shares = serverConfig.getShares

  private val defaultMaxResults = 500

  private def encodePageToken(id: String, share: Option[String], schema: Option[String]): String = {
    val binary = PageToken(id = Option(id), share = share, schema = schema).toByteArray
    new String(Base64.getUrlEncoder().encode(binary), UTF_8)
  }

  private def decodePageToken(
      pageTokenString: String,
      expectedShare: Option[String],
      expectedSchema: Option[String]): String = {
    val pageToken =
      try {
        val binary = Base64.getUrlDecoder().decode(pageTokenString.getBytes(UTF_8))
        PageToken.parseFrom(binary)
      } catch {
        case _: IllegalArgumentException | _: IOException =>
          throw new DeltaSharingIllegalArgumentException("invalid 'nextPageToken'")
      }
    if (pageToken.id.isEmpty
      || pageToken.share != expectedShare
      || pageToken.schema != expectedSchema
    ) {
      throw new DeltaSharingIllegalArgumentException("invalid 'nextPageToken'")
    }
    pageToken.getId
  }

  private def getPage[T](
      nextPageToken: Option[String],
      share: Option[String],
      schema: Option[String],
      maxResults: Option[Int],
      totalSize: Int)(func: (Int, Int) => Seq[T]): (Seq[T], Option[String]) = {
    assertMaxResults(maxResults)
    val start = nextPageToken.map {
      pageToken => decodePageToken(pageToken, share, schema).toInt
    }.getOrElse(0)
    if (start > totalSize) {
      throw new DeltaSharingIllegalArgumentException("invalid 'nextPageToken'")
    }
    val end = start + maxResults.getOrElse(defaultMaxResults)
    val results = func(start, end)
    val nextId = if (end < totalSize) Some(end) else None
    results -> nextId.map(id => encodePageToken(id.toString, share, schema))
  }

  private def assertMaxResults(maxResults: Option[Int]): Unit = {
    maxResults.foreach { m =>
      if (m < 0 || m > defaultMaxResults) {
        throw new DeltaSharingIllegalArgumentException(
          s"Acceptable values of 'maxResults' are 0 to $defaultMaxResults, inclusive. " +
            s"(Default: $defaultMaxResults)")
      }
    }
  }

  private def getShareInternal(share: String): ShareConfig = {
    shares.asScala.find(s => caseInsensitiveComparer(s.getName, share))
      .getOrElse(throw new DeltaSharingNoSuchElementException(s"share '$share' not found"))
  }

  private def getSchema(shareConfig: ShareConfig, schema: String): SchemaConfig = {
    shareConfig.getSchemas.asScala.find(s => caseInsensitiveComparer(s.getName, schema))
      .getOrElse(throw new DeltaSharingNoSuchElementException(s"schema '$schema' not found"))
  }

  def listShares(
      nextPageToken: Option[String] = None,
      maxResults: Option[Int] = None): (Seq[Share], Option[String]) = {
    getPage(nextPageToken, None, None, maxResults, shares.size) { (start, end) =>
      shares.asScala.toSeq.map { share =>
        Share().withName(share.getName)
      }.slice(start, end).seq
    }
  }

  def getShare(share: String): Share = {
    val shareConfig = getShareInternal(share)
    Share().withName(shareConfig.getName)
  }

  def listSchemas(
      share: String,
      nextPageToken: Option[String] = None,
      maxResults: Option[Int] = None): (Seq[Schema], Option[String]) = {
    val shareConfig = getShareInternal(share)
    getPage(nextPageToken, Some(share), None, maxResults, shareConfig.getSchemas.size) {
      (start, end) =>
        shareConfig.getSchemas.asScala.toSeq.map { schemaConfig =>
          Schema().withName(schemaConfig.getName).withShare(share)
        }.slice(start, end)
    }
  }

  def listTables(
      share: String,
      schema: String,
      nextPageToken: Option[String] = None,
      maxResults: Option[Int] = None): (Seq[Table], Option[String]) = {
    val schemaConfig = getSchema(getShareInternal(share), schema)
    getPage(nextPageToken, Some(share), Some(schema), maxResults, schemaConfig.getTables.size) {
      (start, end) =>
        schemaConfig.getTables.asScala.toSeq.map {
          tableConfig =>
            Table(
              name = Some(tableConfig.getName),
              schema = Some(schema),
              share = Some(share),
              id = if (tableConfig.id.isEmpty) None else Some(tableConfig.id)
            )
        }.slice(start, end)
    }
  }

  def listAllTables(
      share: String,
      nextPageToken: Option[String] = None,
      maxResults: Option[Int] = None): (Seq[Table], Option[String]) = {
    val shareConfig = getShareInternal(share)
    val totalSize = shareConfig.schemas.asScala.map(_.tables.size).sum
    getPage(nextPageToken, Some(share), None, maxResults, totalSize) {
      (start, end) =>
        shareConfig.schemas.asScala.toSeq.flatMap { schema =>
          schema.tables.asScala.toSeq.map {
            table =>
              Table(
                name = Some(table.getName),
                schema = Some(schema.name),
                share = Some(share),
                id = if (table.id.isEmpty) None else Some(table.id)
              )
          }
        }.slice(start, end)
    }
  }

  def getTable(share: String, schema: String, table: String): TableConfig = {
    val schemaConfig =
      try {
        getSchema(getShareInternal(share), schema)
      } catch {
        case _: DeltaSharingNoSuchElementException =>
          throw new DeltaSharingNoSuchElementException(
            s"[Share/Schema/Table] '$share/$schema/$table' does not exist, " +
              s"please contact your share provider for further information.")
      }
    schemaConfig.getTables.asScala.find(t => caseInsensitiveComparer(t.getName, table))
      .getOrElse(throw new DeltaSharingNoSuchElementException(
        s"[Share/Schema/Table] '$share/$schema/$table' does not exist, " +
          s"please contact your share provider for further information."))
  }
}
