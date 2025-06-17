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

package io.delta.sharing.client

import java.net.URI

import org.apache.http.{HttpHost, HttpRequest}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpRequestWrapper}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.conn.ClientConnectionManager
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.params.HttpParams
import org.apache.http.protocol.HttpContext

private[sharing] case class DeltaSharingFileSystemHttpClient(
  noProxyHttpClient: CloseableHttpClient,
  proxyHttpClient: CloseableHttpClient,
  useProxy: Boolean,
  noProxyHosts: Seq[String],
  disableHttps: Boolean) extends CloseableHttpClient {

  private[sharing] def hasNoProxyHostsMatch(host: String): Boolean = {
    noProxyHosts.exists(record => {
      // Wildcard DNS records support
      if (record.startsWith("*.")) {
        host.endsWith(record.drop(1))
      } else {
        host == record
      }
    })
  }

  override protected def doExecute(
    target: HttpHost,
    request: HttpRequest,
    context: HttpContext
  ): CloseableHttpResponse = {
    val (updatedTarget, updatedRequest) = if (disableHttps && target.getSchemeName == "https") {
      val modifiedUri: URI = new URIBuilder(request.getRequestLine.getUri).setScheme("http").build()
      val wrappedRequest = HttpRequestWrapper.wrap(request)
      wrappedRequest.setURI(modifiedUri)
      (new HttpHost(target.getHostName, target.getPort, "http"), wrappedRequest)
    } else {
      (target, request)
    }
    if (useProxy && !hasNoProxyHostsMatch(target.getHostName)) {
      proxyHttpClient.execute(updatedTarget, updatedRequest, context)
    } else {
      noProxyHttpClient.execute(updatedTarget, updatedRequest, context)
    }
  }

  override def close(): Unit = {
    noProxyHttpClient.close()
    proxyHttpClient.close()
  }

  // This is deprecated since 4.3, so overriding with a dummy implementation
  override def getParams(): HttpParams = {
    throw new UnsupportedOperationException("getParams")
  }

  // This is deprecated since 4.3, so overriding with a dummy implementation
  override def getConnectionManager(): ClientConnectionManager = {
    throw new UnsupportedOperationException("getConnectionManager")
  }
}
