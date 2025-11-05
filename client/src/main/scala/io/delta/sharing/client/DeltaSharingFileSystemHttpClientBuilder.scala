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

import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.HttpClientBuilder

private[sharing] class DeltaSharingFileSystemHttpClientBuilder {
  private var noProxyHttpClientClientBuilder: HttpClientBuilder = HttpClientBuilder.create()
  private var proxyHttpClientClientBuilder: HttpClientBuilder = HttpClientBuilder.create()
  private var useProxy: Boolean = false
  private var noProxyHosts: Seq[String] = Seq.empty
  private var disableHttps: Boolean = false

  def setMaxConnTotal(maxConnTotal: Int): DeltaSharingFileSystemHttpClientBuilder = {
    noProxyHttpClientClientBuilder.setMaxConnTotal(maxConnTotal)
    proxyHttpClientClientBuilder.setMaxConnTotal(maxConnTotal)
    this
  }

  def setMaxConnPerRoute(maxConnPerRoute: Int): DeltaSharingFileSystemHttpClientBuilder = {
    noProxyHttpClientClientBuilder.setMaxConnPerRoute(maxConnPerRoute)
    proxyHttpClientClientBuilder.setMaxConnPerRoute(maxConnPerRoute)
    this
  }

  def setDefaultRequestConfig(config: RequestConfig): DeltaSharingFileSystemHttpClientBuilder = {
    noProxyHttpClientClientBuilder.setDefaultRequestConfig(config)
    proxyHttpClientClientBuilder.setDefaultRequestConfig(config)
    this
  }

  def disableAutomaticRetries(): DeltaSharingFileSystemHttpClientBuilder = {
    noProxyHttpClientClientBuilder.disableAutomaticRetries()
    proxyHttpClientClientBuilder.disableAutomaticRetries()
    this
  }

  def setProxy(proxy: HttpHost): DeltaSharingFileSystemHttpClientBuilder = {
    proxyHttpClientClientBuilder.setProxy(proxy)
    useProxy = true
    this
  }

  def setNoProxyHosts(noProxyHosts: Seq[String]): DeltaSharingFileSystemHttpClientBuilder = {
    this.noProxyHosts = noProxyHosts
    this
  }

  def setDisableHttps(): DeltaSharingFileSystemHttpClientBuilder = {
    disableHttps = true
    this
  }

  def build(): DeltaSharingFileSystemHttpClient = {
    DeltaSharingFileSystemHttpClient(
      noProxyHttpClient = noProxyHttpClientClientBuilder.build(),
      proxyHttpClient = proxyHttpClientClientBuilder.build(),
      useProxy = useProxy,
      noProxyHosts = noProxyHosts,
      disableHttps = disableHttps
    )
  }
}

private[sharing] object DeltaSharingFileSystemHttpClientBuilder {
  def create(): DeltaSharingFileSystemHttpClientBuilder = {
    new DeltaSharingFileSystemHttpClientBuilder
  }
}
