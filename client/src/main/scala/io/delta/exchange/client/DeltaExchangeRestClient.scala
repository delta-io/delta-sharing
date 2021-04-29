package io.delta.exchange.client

import java.io.InputStream
import java.net.{URI, URL}

import org.apache.http.{HttpHeaders, HttpHost}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpEntityEnclosingRequestBase, HttpGet, HttpRequestBase}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{HttpClientBuilder, HttpClients}
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, SSLContextBuilder, TrustSelfSignedStrategy}

import scala.io.Source

class DeltaExchangeRestClient(
    profileProvider: DeltaExchangeProfileProvider,
    sslTrustAll: Boolean = false) extends DeltaExchangeClient {

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
    clientBuilder.build()
  }

  override def getTableInfo(request: GetTableInfoRequest): GetTableInfoResponse = {
    getInternal[GetTableInfoRequest, GetTableInfoResponse]("/s3commit/table/info", Some(request))
  }

  override def getMetadata(request: GetMetadataRequest): GetMetadataResponse = {
    getInternal[GetMetadataRequest, GetMetadataResponse]("/s3commit/table/metadata", Some(request))
  }

  override def getFiles(request: GetFilesRequest): GetFilesResponse = {
    getInternal[GetFilesRequest,GetFilesResponse]("/s3commit/table/files", Some(request))
  }

  private def getPath(target: String): String = {
    s"/api/2.0/${target.stripPrefix("/")}"
  }

  private def getInternal[T: Manifest, R: Manifest](target: String, data: Option[T]): R = {
    val path = getPath(target)
    val response = if (data.isEmpty) {
      httpRequestInternal(new HttpGet(path))
    } else {
      val customGet = new HttpGetWithEntity()
      customGet.setURI(new URI(path))
      val json = JsonUtils.toJson(data.get)
      customGet.setHeader("Content-type", "application/json")
      customGet.setEntity(new StringEntity(json, "UTF-8"))
      httpRequestInternal(customGet)
    }
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
    client.close()
  }

  override def listShares(): ListSharesResponse = {
    getInternal[Any, ListSharesResponse]("/s3commit/table/shares", None)
  }

  override def getShare(request: GetShareRequest): GetShareResponse = {
    getInternal[GetShareRequest, GetShareResponse]("/s3commit/table/share", Some(request))
  }
}

class HttpGetWithEntity extends HttpEntityEnclosingRequestBase {
  def getMethod(): String = "GET"
}

class UnexpectedHttpError(message: String, val statusCode: Int)
  extends IllegalStateException(message)
