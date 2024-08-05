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
package io.delta.sharing.client.auth

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.http.impl.client.HttpClients
import java.net.InetSocketAddress
import java.io.OutputStream

import org.apache.spark.SparkFunSuite

class OAuthClientSuite extends SparkFunSuite {

  var server: HttpServer = _

  def startServer(): Unit = {
    server = HttpServer.create(new InetSocketAddress(1080), 0)
    server.createContext("/token", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val response = """{
                         | "access_token": "test-access-token",
                         | "expires_in": 3600,
                         | "token_type": "bearer"
                         |}""".stripMargin
        exchange.sendResponseHeaders(200, response.getBytes.length)
        val os: OutputStream = exchange.getResponseBody
        os.write(response.getBytes)
        os.close()
      }
    })
    server.setExecutor(null)
    server.start()
  }

  def stopServer(): Unit = {
    if (server != null) server.stop(0)
  }

  override def beforeAll(): Unit = {
    startServer()
  }

  override def afterAll(): Unit = {
    stopServer()
  }

  test("OAuthClient should parse token response correctly") {
    val httpClient = HttpClients.createDefault()
    val oauthClient = new OAuthClient(httpClient, "http://localhost:1080/token", "client-id", "client-secret")

    val start = System.currentTimeMillis()

    val token = oauthClient.clientCredentials()

    val end = System.currentTimeMillis()

    assert(token.accessToken == "test-access-token")
    assert(token.expiresIn == 3600)
    assert(token.creationTimestamp >= start && token.creationTimestamp <= end)
  }
}