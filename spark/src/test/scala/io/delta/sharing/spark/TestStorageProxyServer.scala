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

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.util.Try

import org.sparkproject.jetty.client.HttpClient
import org.sparkproject.jetty.http.HttpMethod
import org.sparkproject.jetty.server.{Request, Server}
import org.sparkproject.jetty.server.handler.AbstractHandler
import org.sparkproject.jetty.util.ssl.SslContextFactory


/**
 * A simple proxy server that forwards storage access while upgrading the connection to https.
 * This is used to test the behavior of the DeltaSharingFileSystem when
 * "spark.delta.sharing.never.use.https" is set to true.
 */
class TestStorageProxyServer {
  private val server = new Server(0)
  val sslContextFactory = new SslContextFactory.Client()
  private val httpClient = new HttpClient(sslContextFactory)
  server.setHandler(new ProxyHandler)

  def initialize(): Unit = {
    new Thread(() => {
      Try(httpClient.start())
      Try(server.start())
    }).start()

    do {
      Thread.sleep(100)
    } while (!server.isStarted())
  }

  def stop(): Unit = {
    Try(server.stop())
    Try(httpClient.stop())
  }

  def getPort(): Int = {
    server.getURI().getPort()
  }

  def getHost(): String = {
    server.getURI().getHost
  }

  private class ProxyHandler extends AbstractHandler {
    override def handle(target: String,
                        baseRequest: Request,
                        request: HttpServletRequest,
                        response: HttpServletResponse): Unit = {

      Option(request.getHeader("Host")) match {
        case Some(host) =>
          // upgrade bucket access call from http -> https
          val uri = "https://" + host + request.getRequestURI.replace("null", "") +
            "?" + request.getQueryString

          val res = httpClient.newRequest(uri)
            .method(HttpMethod.GET)
            .header("Range", request.getHeader("Range"))
            .send()

          response.setStatus(res.getStatus)
          res.getHeaders.forEach { header =>
            response.setHeader(header.getName, header.getValue)
          }
          val out = response.getOutputStream
          out.write(res.getContent, 0, res.getContent.length)
          out.flush()
          out.close()

          baseRequest.setHandled(true)

        case None =>
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No forwarding URL provided")
      }
    }
  }
}
