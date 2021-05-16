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

package io.delta.sharing.spark

import io.delta.sharing.spark.model._
import java.io.InputStream
import java.net.{URL, URLEncoder}

import io.delta.sharing.spark.util.JsonUtils
import org.apache.http.{HttpHeaders, HttpHost}
import org.apache.http.client.methods.{HttpGet, HttpHead, HttpPost, HttpRequestBase}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{HttpClientBuilder, HttpClients}
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, SSLContextBuilder, TrustSelfSignedStrategy}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

trait DeltaSharingClient {
  def listAllTables(): Seq[Table]

  def getTableVersion(table: Table): Long

  def getMetadata(table: Table): DeltaTableMetadata

  def getFiles(table: Table, predicates: Seq[String], limit: Option[Int]): DeltaTableFiles
}

trait PaginationResponse {
  def nextPageToken: Option[String]
}

case class QueryTableRequest(predicateHints: Seq[String], limitHint: Option[Int])

case class ListSharesResponse(
    items: Seq[Share],
    nextPageToken: Option[String]) extends PaginationResponse

case class ListSchemasResponse(
    items: Seq[Schema],
    nextPageToken: Option[String]) extends PaginationResponse

case class ListTablesResponse(
    items: Seq[Table],
    nextPageToken: Option[String]) extends PaginationResponse

class DeltaSharingRestClient(
    profileProvider: DeltaSharingProfileProvider,
    sslTrustAll: Boolean = false) extends DeltaSharingClient {

  @volatile private var created = false

  private lazy val client = {
    val clientBuilder: HttpClientBuilder = if (sslTrustAll) {
      val sslBuilder = new SSLContextBuilder()
        .loadTrustMaterial(null, new TrustSelfSignedStrategy())
      val sslsf = new SSLConnectionSocketFactory(
        sslBuilder.build(),
        SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER
      )
      HttpClients.custom().setSSLSocketFactory(sslsf)
    } else {
      HttpClientBuilder.create()
    }
    val client = clientBuilder.build()
    created = true
    client
  }

  override def listAllTables(): Seq[Table] = {
    listShares().flatMap(listSchemas).flatMap(listTables)
  }

  private def getTargetUrl(suffix: String): String = {
    s"${profileProvider.getProfile.endpoint.stripSuffix("/")}/${suffix.stripPrefix("/")}"
  }

  private def listShares(): Seq[Share] = {
    val target = getTargetUrl("shares")
    val shares = ArrayBuffer[Share]()
    var response = getJson[ListSharesResponse](target)
    shares ++= response.items
    while (response.nextPageToken.nonEmpty) {
      val target = getTargetUrl("/shares?pageToken=${response.nextPageToken.get}")
      response = getJson[ListSharesResponse](target)
      shares ++= response.items
    }
    shares
  }

  private def listSchemas(share: Share): Seq[Schema] = {
    val encodedShareName = URLEncoder.encode(share.name, "UTF-8")
    val target = getTargetUrl(s"/shares/$encodedShareName/schemas")
    val schemas = ArrayBuffer[Schema]()
    var response = getJson[ListSchemasResponse](target)
    schemas ++= response.items
    while (response.nextPageToken.nonEmpty) {
      val target =
        getTargetUrl(s"/shares/$encodedShareName/schemas?pageToken=${response.nextPageToken.get}")
      response = getJson[ListSchemasResponse](target)
      schemas ++= response.items
    }
    schemas
  }

  private def listTables(schema: Schema): Seq[Table] = {
    val encodedShareName = URLEncoder.encode(schema.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(schema.name, "UTF-8")
    val target = getTargetUrl(s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables")
    val tables = ArrayBuffer[Table]()
    var response = getJson[ListTablesResponse](target)
    tables ++= response.items
    while (response.nextPageToken.nonEmpty) {
      val target = getTargetUrl(s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables" +
        s"?pageToken=${response.nextPageToken.get}")
      response = getJson[ListTablesResponse](target)
      tables ++= response.items
    }
    tables
  }

  override def getTableVersion(table: Table): Long = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target =
      getTargetUrl(s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName")
    val (version, _) = getResponse(new HttpHead(target))
    version.getOrElse {
      throw new IllegalStateException("Cannot find Delta-Table-Version in the header")
    }
  }

  def getMetadata(table: Table): DeltaTableMetadata = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target = getTargetUrl(
      s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName/metadata")
    val (version, lines) = getNDJson(target)
    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
    if (lines.size != 2) {
      throw new IllegalStateException("received more than two lines")
    }
    DeltaTableMetadata(version, protocol, metadata)
  }

  private def checkProtocol(protocol: Protocol): Unit = {
    if (protocol.minReaderVersion > DeltaSharingRestClient.CURRENT) {
      throw new IllegalArgumentException(s"The table requires a newer version" +
        s" (${protocol.minReaderVersion}) to read. But the current release supports version " +
        s"is ${DeltaSharingProfile.CURRENT} and below. Please upgrade to a newer release.")
    }
  }

  override def getFiles(
      table: Table,
      predicates: Seq[String],
      limit: Option[Int]): DeltaTableFiles = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target = getTargetUrl(
      s"/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName/query")
    val (version, lines) = getNDJson(target, QueryTableRequest(predicates, limit))
    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    checkProtocol(protocol)
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
    val files = lines.drop(2).map(line => JsonUtils.fromJson[SingleAction](line).file)
    DeltaTableFiles(version, protocol, metadata, files)
  }

  private def getNDJson(target: String): (Long, Seq[String]) = {
    val (version, response) = getResponse(new HttpGet(target))
    version.getOrElse {
      throw new IllegalStateException("Cannot find Delta-Table-Version in the header")
    } -> response.split("[\n\r]+")
  }

  private def getNDJson[T: Manifest](target: String, data: T): (Long, Seq[String]) = {
    val httpPost = new HttpPost(target)
    val json = JsonUtils.toJson(data)
    httpPost.setHeader("Content-type", "application/json")
    httpPost.setEntity(new StringEntity(json, "UTF-8"))
    val (version, response) = getResponse(httpPost)
    version.getOrElse {
      throw new IllegalStateException("Cannot find Delta-Table-Version in the header")
    } -> response.split("[\n\r]+")
  }

  private def getJson[R: Manifest](target: String): R = {
    val (_, response) = getResponse(new HttpGet(target))
    JsonUtils.fromJson[R](response)
  }

  private def getHttpHost(endpoint: String): HttpHost = {
    val url = new URL(endpoint)
    val protocol = url.getProtocol
    val port = if (url.getPort == -1) {
      if (protocol == "https") 443 else 80
    } else {
      url.getPort
    }
    new HttpHost(url.getHost, port, protocol)
  }

  private def getResponse(httpContext: HttpRequestBase): (Option[Long], String) = {
    val profile = profileProvider.getProfile
    httpContext.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer ${profile.bearerToken}")
    val response =
      client.execute(getHttpHost(profile.endpoint), httpContext, HttpClientContext.create())
    try {
      val status = response.getStatusLine()
      val entity = response.getEntity()
      val body = if (entity == null) {
        ""
      } else {
        val is: InputStream = entity.getContent()
        try {
          val res = Source.fromInputStream(is).mkString
          res
        } finally {
          is.close()
        }
      }

      val statusCode = status.getStatusCode

      if (statusCode != 200) {
        throw new UnexpectedHttpStatus(
          s"HTTP request failed with status: $status $body",
          statusCode)
      }
      Option(response.getFirstHeader("Delta-Table-Version")).map(_.getValue.toLong) -> body
    } finally {
      response.close()
    }
  }

  def close(): Unit = {
    if (created) {
      try {
        client.close()
      } finally {
        created = false
      }
    }
  }

  override def finalize(): Unit = {
    try {
      close()
    } finally {
      super.finalize()
    }
  }
}

object DeltaSharingRestClient {
  val CURRENT = 1
}

class UnexpectedHttpStatus(message: String, val statusCode: Int)
  extends IllegalStateException(message)
