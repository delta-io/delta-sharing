package io.delta.exchange.client

import io.delta.exchange.client.model._
import java.io.InputStream
import java.net.{URI, URL, URLEncoder}

import org.apache.http.{HttpHeaders, HttpHost}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpEntityEnclosingRequestBase, HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{HttpClientBuilder, HttpClients}
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, SSLContextBuilder, TrustSelfSignedStrategy}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

trait PaginationResponse {
  def nextPageToken: Option[String]
}

case class QueryTableRequest(predicateHints: Seq[String], limitHint: Option[Int])

case class ListSharesResponse(items: Seq[Share], nextPageToken: Option[String]) extends PaginationResponse

case class ListSchemasResponse(items: Seq[Schema], nextPageToken: Option[String]) extends PaginationResponse

case class ListTablesResponse(items: Seq[Table], nextPageToken: Option[String]) extends PaginationResponse

class DeltaExchangeRestClient(
    profileProvider: DeltaExchangeProfileProvider,
    sslTrustAll: Boolean = false) extends DeltaExchangeClient {

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

  private def listShares(): Seq[Share] = {
    val target = s"${profileProvider.getProfile.endpoint.stripSuffix("/")}/shares"
    val shares = ArrayBuffer[Share]()
    var response = getInternal[ListSharesResponse](target)
    shares ++= response.items
    while (response.nextPageToken.nonEmpty) {
      val target = s"${profileProvider.getProfile.endpoint.stripSuffix("/")}/shares?pageToken=${response.nextPageToken.get}"
      response = getInternal[ListSharesResponse](target)
      shares ++= response.items
    }
    shares
  }

  private def listSchemas(share: Share): Seq[Schema] = {
    val encodedShareName = URLEncoder.encode(share.name, "UTF-8")
    val target = s"${profileProvider.getProfile.endpoint.stripSuffix("/")}/shares/$encodedShareName/schemas"
    val schemas = ArrayBuffer[Schema]()
    var response = getInternal[ListSchemasResponse](target)
    schemas ++= response.items
    while (response.nextPageToken.nonEmpty) {
      val target = s"${profileProvider.getProfile.endpoint.stripSuffix("/")}/shares/$encodedShareName/schemas?pageToken=${response.nextPageToken.get}"
      response = getInternal[ListSchemasResponse](target)
      schemas ++= response.items
    }
    schemas
  }

  private def listTables(schema: Schema): Seq[Table] = {
    val encodedShareName = URLEncoder.encode(schema.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(schema.name, "UTF-8")
    val target = s"${profileProvider.getProfile.endpoint.stripSuffix("/")}/shares/$encodedShareName/schemas/$encodedSchemaName/tables"
    val tables = ArrayBuffer[Table]()
    var response = getInternal[ListTablesResponse](target)
    tables ++= response.items
    while (response.nextPageToken.nonEmpty) {
      val target = s"${profileProvider.getProfile.endpoint.stripSuffix("/")}/shares/$encodedShareName/schemas/$encodedSchemaName/tables?pageToken=${response.nextPageToken.get}"
      response = getInternal[ListTablesResponse](target)
      tables ++= response.items
    }
    tables
  }

  def getMetadata(table: Table): DeltaTableMetadata = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target = s"${profileProvider.getProfile.endpoint.stripSuffix("/")}/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName/metadata"
    val lines = getNDJson(target)
    lines.foreach(println)
    assert(lines.size == 2)
    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
    DeltaTableMetadata(protocol, metadata)
  }

  override def getFiles(table: Table, predicates: Seq[String], limit: Option[Int]): DeltaTableFiles = {
    val encodedShareName = URLEncoder.encode(table.share, "UTF-8")
    val encodedSchemaName = URLEncoder.encode(table.schema, "UTF-8")
    val encodedTableName = URLEncoder.encode(table.name, "UTF-8")
    val target = s"${profileProvider.getProfile.endpoint.stripSuffix("/")}/shares/$encodedShareName/schemas/$encodedSchemaName/tables/$encodedTableName/query"
    val lines = getNDJson(target, QueryTableRequest(predicates, limit))
    assert(lines.size >= 2)
    val protocol = JsonUtils.fromJson[SingleAction](lines(0)).protocol
    val metadata = JsonUtils.fromJson[SingleAction](lines(1)).metaData
    val files = lines.drop(2).map(line => JsonUtils.fromJson[SingleAction](line).add)
    DeltaTableFiles(protocol, metadata, files)
  }

//  override def getTableInfo(request: GetTableInfoRequest): GetTableInfoResponse = {
//    getInternal[GetTableInfoRequest, GetTableInfoResponse]("/s3commit/table/info", Some(request))
//  }
//
//  override def getMetadata(request: GetMetadataRequest): GetMetadataResponse = {
//    getInternal[GetMetadataRequest, GetMetadataResponse]("/s3commit/table/metadata", Some(request))
//  }
//
//  override def getFiles(request: GetFilesRequest): GetFilesResponse = {
//    getInternal[GetFilesRequest,GetFilesResponse]("/s3commit/table/files", Some(request))
//  }

  private def getNDJson(target: String): Seq[String] = {
    httpRequestInternal(new HttpGet(target)).split("[\n\r]+")
  }

  private def getNDJson[T: Manifest](target: String, data: T): Seq[String] = {
    val httpPost = new HttpPost(target)
    val json = JsonUtils.toJson(data)
    httpPost.setHeader("Content-type", "application/json")
    httpPost.setEntity(new StringEntity(json, "UTF-8"))
    httpRequestInternal(httpPost).split("[\n\r]+")
  }

  private def getInternal[R: Manifest](target: String): R = {
    val response = httpRequestInternal(new HttpGet(target))
    println("response: " + response)
    JsonUtils.fromJson[R](response)
  }

  private def postInternal[T: Manifest, R: Manifest](target: String, data: T): R = {
    val httpPost = new HttpPost(target)
    val json = JsonUtils.toJson(data)
    httpPost.setHeader("Content-type", "application/json")
    httpPost.setEntity(new StringEntity(json, "UTF-8"))
    val response = httpRequestInternal(httpPost)
    println("response: " + response)
    JsonUtils.fromJson[R](response)
  }

  private def httpRequestInternal(httpContext: HttpRequestBase): String = {
    val profile = profileProvider.getProfile
    httpContext.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer ${profile.token}")
    println("httpContext: " + httpContext)
    getResponseBody(client.execute(getHttpHost(profile.endpoint), httpContext, HttpClientContext.create()))
  }

  private def getHttpHost(endpoint: String): HttpHost = {
    val url = new URL(endpoint)
    val protocol = url.getProtocol
    val port = if (url.getPort == -1) {
      if (protocol == "https") 443 else 80
    } else {
      url.getPort
    }
    val host = url.getHost

    new HttpHost(host, port, protocol)
  }

  private def getResponseBody(response: CloseableHttpResponse): String = {
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
        throw new UnexpectedHttpError(s"HTTP request failed with status: $status $body", statusCode)
      }
      body
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

//  override def listShares(): ListSharesResponse = {
//    getInternal[Any, ListSharesResponse]("/s3commit/table/shares", None)
//  }

//  override def getShare(request: GetShareRequest): GetShareResponse = {
//    getInternal[GetShareRequest, GetShareResponse]("/s3commit/table/share", Some(request))
//  }
}

class HttpGetWithEntity extends HttpEntityEnclosingRequestBase {
  def getMethod(): String = "GET"
}

class UnexpectedHttpError(message: String, val statusCode: Int)
  extends IllegalStateException(message)
