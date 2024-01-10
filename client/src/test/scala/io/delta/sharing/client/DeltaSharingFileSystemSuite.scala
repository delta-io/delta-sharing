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
import org.apache.http.impl.client.CloseableHttpClient
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
    // Step 1: Create a local HTTP server
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

    // Step 2: Create a local HTTP proxy server
    // Please replace this with actual code to create a proxy server
    val proxyServer = new ProxyServer(0)
    proxyServer.initialize()
    try {
      // Step 3: Create a ProxyConfig with the host and port of the local proxy server
      val conf = new Configuration
      val path = DeltaSharingPath("https://delta.io/foo", "myid", 100).toPath
      conf.set(ConfUtils.PROXY_HOST, "localhost")
      conf.set(ConfUtils.PROXY_PORT, proxyServer.getPort().toString)
      val fs = path.getFileSystem(conf)

      // Step 4: Use reflection to access the httpClient field in DeltaSharingFileSystem
      val methodName = "createHttpClient"  // replace with your method name

      val method = fs.getClass.getDeclaredMethod(methodName)
      method.setAccessible(true)
      val httpClient = method.invoke(fs)
        .asInstanceOf[CloseableHttpClient]
      // Step 5: Configure the httpClient to use the ProxyConfig
      // Please replace this with actual code to configure the httpClient

      // Step 6: Send a request to the local server through the httpClient
      val response = httpClient.execute(new HttpGet(server.getURI.toString))

      // Step 7: Assert that the request is successful
      assert(response.getStatusLine.getStatusCode == HttpServletResponse.SC_OK)
      val content = EntityUtils.toString(response.getEntity)
      assert(content.trim == "Hello, World!")
    } finally {
      server.stop()
      proxyServer.stop()
    }
  }
}
