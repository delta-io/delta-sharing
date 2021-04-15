package io.delta.exchange.spark

import java.io.InputStream
import java.net.{URI, URL}

import org.apache.hadoop.fs.Path
import org.apache.http.{HttpHeaders, HttpHost}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpEntityEnclosingRequestBase, HttpGet, HttpRequestBase}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{HttpClientBuilder, HttpClients}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, SSLContextBuilder, TrustSelfSignedStrategy}

import scala.io.Source

case class GetTableInfoRequest(uuid: String)

case class GetTableInfoResponse(path: String, version: Long)

case class GetMetadataRequest(uuid: String, version: Long)

// metadata is the json format of Metadata
case class GetMetadataResponse(metadata: String)

case class GetFilesRequest(uuid: String, version: Long, partitionFilter: Option[String])

// file is the json format of AddFile whose path is a pre-signed url
case class GetFilesResponse(file: Seq[String])

trait DeltaLogClient {
  def getTableInfo(request: GetTableInfoRequest): GetTableInfoResponse

  def getMetadata(request: GetMetadataRequest): GetMetadataResponse

  def getFiles(request: GetFilesRequest): GetFilesResponse
}


/** A test class that signs urls locally */
class DeltaLogLocalClient(deltaLog: DeltaLog, signer: CloudFileSigner) extends DeltaLogClient {

  override def getTableInfo(request: GetTableInfoRequest): GetTableInfoResponse = {
    GetTableInfoResponse(deltaLog.dataPath.toString.replace("file", "delta"), deltaLog.snapshot.version)
  }

  override def getMetadata(request: GetMetadataRequest): GetMetadataResponse = {
    GetMetadataResponse(JsonUtils.toJson(deltaLog.getSnapshotAt(request.version).metadata))
  }

  override def getFiles(request: GetFilesRequest): GetFilesResponse = {
    val addFiles = deltaLog.getSnapshotAt(request.version).allFiles.collect()
    GetFilesResponse(addFiles.map(signAddFile).map(a => JsonUtils.toJson(a)))
  }

  protected def absolutePath(child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      new Path(deltaLog.dataPath, p)
    }
  }

  protected def signAddFile(addFile: AddFile): AddFile = {
    val uri = absolutePath(addFile.path).toUri
    val bucket = if (uri.getUserInfo == null || uri.getUserInfo.isEmpty) {
      uri.getHost
    } else {
      uri.getUserInfo
    }
    val objectKey = uri.getPath.substring(1)
    addFile.copy(signer.sign(bucket, objectKey).toURI.toString)
  }
}

/**
 * A hacky client talking to the server built by https://github.com/databricks/universe/pull/90204
 */
class DeltaLogRestClient(apiUrl: URL, apiToken: String) extends DeltaLogClient {

  val client = {
    val sslTrustAll = apiUrl.getHost == "localhost" || apiUrl.getHost == "localhost:443"
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
    httpContext.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $apiToken")
    println("httpContext: " + httpContext)
    getResponseBody(client.execute(getHttpHost(apiUrl), httpContext, HttpClientContext.create()))
  }

  private def getHttpHost(endpoint: URL): HttpHost = {
    val protocol = endpoint.getProtocol
    val port = if (endpoint.getPort == -1) {
      if (protocol == "https") 443 else 80
    } else {
      endpoint.getPort
    }
    val host = endpoint.getHost

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
}

class HttpGetWithEntity extends HttpEntityEnclosingRequestBase {
  def getMethod(): String = "GET"
}

class UnexpectedHttpError(message: String, val statusCode: Int)
  extends IllegalStateException(message)
