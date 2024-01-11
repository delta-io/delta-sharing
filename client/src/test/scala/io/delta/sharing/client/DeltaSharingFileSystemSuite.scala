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

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.apache.hadoop.conf.Configuration
import org.apache.http.client.methods.HttpGet
import org.apache.http.util.EntityUtils
import org.apache.spark.SparkFunSuite
import org.sparkproject.jetty.server.Server
import org.sparkproject.jetty.servlet.{ServletHandler, ServletHolder}

import io.delta.sharing.client.model._
import io.delta.sharing.client.util.{ConfUtils, ProxyServer}

class DeltaSharingFileSystemSuite extends SparkFunSuite {
  import DeltaSharingFileSystem._

  test("encode and decode") {
    val tablePath = "https://delta.io/foo"

    val actions: Seq[FileAction] = Seq(
      AddFile("unused", "id", Map.empty, 100),
      AddFileForCDF("unused_cdf", "id_cdf", Map.empty, 200, 1, 2),
      AddCDCFile("unused_cdc", "id_cdc", Map.empty, 300, 1, 2),
      RemoveFile("unused_rem", "id_rem", Map.empty, 400, 1, 2)
    )

    actions.foreach ( action => {
      assert(decode(encode(tablePath, action)) ==
        DeltaSharingPath("https://delta.io/foo", action.id, action.size))
    })
  }

  test("file system should be cached") {
    val tablePath = "https://delta.io/foo"
    val actions: Seq[FileAction] = Seq(
      AddFile("unused", "id", Map.empty, 100),
      AddFileForCDF("unused_cdf", "id_cdf", Map.empty, 200, 1, 2),
      AddCDCFile("unused_cdc", "id_cdc", Map.empty, 300, 1, 2),
      RemoveFile("unused_rem", "id_rem", Map.empty, 400, 1, 2)
    )

    actions.foreach( action => {
      val path = encode(tablePath, action)
      val conf = new Configuration
      val fs = path.getFileSystem(conf)
      assert(fs.isInstanceOf[DeltaSharingFileSystem])
      assert(fs eq path.getFileSystem(conf))
    })
  }

  test("traffic goes through a proxy when a proxy configured") {
    // Create a local HTTP server.
    val server = new Server(0)
    val handler = new ServletHandler()
    server.setHandler(handler)
    handler.addServletWithMapping(new ServletHolder(new HttpServlet {
      override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        resp.setContentType("text/plain")
        resp.setStatus(HttpServletResponse.SC_OK)

        // scalastyle:off println
        resp.getWriter.println("Hello, World!")
        // scalastyle:on println
      }
    }), "/*")
    server.start()
    do {
      Thread.sleep(100)
    } while (!server.isStarted())

    // Create a local HTTP proxy server.
    val proxyServer = new ProxyServer(0)
    proxyServer.initialize()

    try {

      // Create a ProxyConfig with the host and port of the local proxy server.
      val conf = new Configuration
      conf.set(ConfUtils.PROXY_HOST, proxyServer.getHost())
      conf.set(ConfUtils.PROXY_PORT, proxyServer.getPort().toString)

      // Configure the httpClient to use the ProxyConfig.
      val fs = new DeltaSharingFileSystem() {
        override def getConf = {
          conf
        }
      }

      // Get http client instance.
      val httpClient = fs.createHttpClient()

      // Send a request to the local server through the httpClient.
      val response = httpClient.execute(new HttpGet(server.getURI.toString))

      // Assert that the request is successful.
      assert(response.getStatusLine.getStatusCode == HttpServletResponse.SC_OK)
      val content = EntityUtils.toString(response.getEntity)
      assert(content.trim == "Hello, World!")

      // Assert that the request is passed through proxy.
      assert(proxyServer.getCapturedRequests().size == 1)
    } finally {
      server.stop()
      proxyServer.stop()
    }
  }

  test("traffic skips the proxy when a noProxyHosts configured") {
    // Create a local HTTP server.
    val server = new Server(0)
    val handler = new ServletHandler()
    server.setHandler(handler)
    handler.addServletWithMapping(new ServletHolder(new HttpServlet {
      override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        resp.setContentType("text/plain")
        resp.setStatus(HttpServletResponse.SC_OK)

        // scalastyle:off println
        resp.getWriter.println("Hello, World!")
        // scalastyle:on println
      }
    }), "/*")
    server.start()
    do {
      Thread.sleep(100)
    } while (!server.isStarted())

    // Create a local HTTP proxy server.
    val proxyServer = new ProxyServer(0)
    proxyServer.initialize()
    try {
      // Create a ProxyConfig with the host and port of the local proxy server and noProxyHosts.
      val conf = new Configuration
      conf.set(ConfUtils.PROXY_HOST, proxyServer.getHost())
      conf.set(ConfUtils.PROXY_PORT, proxyServer.getPort().toString)
      conf.set(ConfUtils.NO_PROXY_HOSTS, server.getURI.getHost)

      // Configure the httpClient to use the ProxyConfig.
      val fs = new DeltaSharingFileSystem() {
        override def getConf = {
          conf
        }
      }

      // Get http client instance.
      val httpClient = fs.createHttpClient()

      // Send a request to the local server through the httpClient.
      val response = httpClient.execute(new HttpGet(server.getURI.toString))

      // Assert that the request is successful.
      assert(response.getStatusLine.getStatusCode == HttpServletResponse.SC_OK)
      val content = EntityUtils.toString(response.getEntity)
      assert(content.trim == "Hello, World!")

      // Assert that the request is not passed through proxy.
      assert(proxyServer.getCapturedRequests().isEmpty)
    } finally {
      server.stop()
      proxyServer.stop()
    }
  }

  test("traffic goes through the proxy when noProxyHosts does not include destination") {
    // Create a local HTTP server.
    val server = new Server(0)
    val handler = new ServletHandler()
    server.setHandler(handler)
    handler.addServletWithMapping(new ServletHolder(new HttpServlet {
      override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        resp.setContentType("text/plain")
        resp.setStatus(HttpServletResponse.SC_OK)

        // scalastyle:off println
        resp.getWriter.println("Hello, World!")
        // scalastyle:on println
      }
    }), "/*")
    server.start()
    do {
      Thread.sleep(100)
    } while (!server.isStarted())

    // Create a local HTTP proxy server.
    val proxyServer = new ProxyServer(0)
    proxyServer.initialize()
    try {
      // Create a ProxyConfig with the host and port of the local proxy server and noProxyHosts.
      val conf = new Configuration
      conf.set(ConfUtils.PROXY_HOST, proxyServer.getHost())
      conf.set(ConfUtils.PROXY_PORT, proxyServer.getPort().toString)
      conf.set(ConfUtils.NO_PROXY_HOSTS, "1.2.3.4")

      // Configure the httpClient to use the ProxyConfig.
      val fs = new DeltaSharingFileSystem() {
        override def getConf = {
          conf
        }
      }

      // Get http client instance.
      val httpClient = fs.createHttpClient()

      // Send a request to the local server through the httpClient.
      val response = httpClient.execute(new HttpGet(server.getURI.toString))

      // Assert that the request is successful.
      assert(response.getStatusLine.getStatusCode == HttpServletResponse.SC_OK)
      val content = EntityUtils.toString(response.getEntity)
      assert(content.trim == "Hello, World!")

      // Assert that the request is not passed through proxy.
      assert(proxyServer.getCapturedRequests().size == 1)
    } finally {
      server.stop()
      proxyServer.stop()
    }
  }
}
