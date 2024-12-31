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

import java.io.IOException
import java.util.concurrent.TimeUnit

import org.apache.http.{HttpException, HttpRequest, HttpResponse}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.bootstrap.{HttpServer, ServerBootstrap}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.protocol.{HttpContext, HttpRequestHandler}
import org.apache.spark.SparkFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class OAuthClientSuite extends SparkFunSuite with TableDrivenPropertyChecks {
  var server: HttpServer = _

  def startServer(handler: HttpRequestHandler): Unit = {
    server = ServerBootstrap.bootstrap()
      .setListenerPort(1080)
      .registerHandler("/token", handler)
      .create()

    server.start()
  }

  def stopServer(): Unit = {
    if (server != null) {
      server.shutdown(5, TimeUnit.SECONDS)
      server = null
    }
    waitForPortRelease(1080, 10000)
  }

  def waitForPortRelease(port: Int, timeoutMillis: Int): Unit = {
    val startTime = System.currentTimeMillis()
    while (System.currentTimeMillis() - startTime < timeoutMillis) {
      try {
        new java.net.Socket("localhost", port).close()
      } catch {
        case _: java.net.ConnectException => return // Port is released
      }
      Thread.sleep(100) // Wait for 100 milliseconds before checking again
    }
    throw new RuntimeException(s"Port $port is not released after $timeoutMillis milliseconds")
  }

  case class TokenExchangeSuccessScenario(responseBody: String,
                                          expectedAccessToken: String,
                                          expectedExpiresIn: Long)

  // OAuth spec requires 'expires_in' to be an integer, e.g., 3600.
  // See https://datatracker.ietf.org/doc/html/rfc6749#section-5.1
  // But some token endpoints return `expires_in` as a string e.g., "3600".
  // This test ensures the client can handle such cases.
  // The test case ensures that we support both integer and string values for 'expires_in' field.
  private val tokenExchangeSuccessScenarios = Table(
    "testScenario",
    TokenExchangeSuccessScenario(
      responseBody = """{
                       | "access_token": "test-access-token",
                       | "expires_in": 3600,
                       | "token_type": "bearer"
                       |}""".stripMargin,
      expectedAccessToken = "test-access-token",
      expectedExpiresIn = 3600
    ),
    TokenExchangeSuccessScenario(
      responseBody = """{
                       | "access_token": "test-access-token",
                       | "expires_in": "3600",
                       | "token_type": "bearer"
                       |}""".stripMargin,
      expectedAccessToken = "test-access-token",
      expectedExpiresIn = 3600
    )
  )

  forAll(tokenExchangeSuccessScenarios) { testScenario =>
    test("OAuthClient should parse token response correctly") {
      val handler = new HttpRequestHandler {
        @throws[HttpException]
        @throws[IOException]
        override def handle(request: HttpRequest,
                            response: HttpResponse,
                            context: HttpContext): Unit = {
          response.setEntity(
            new StringEntity(testScenario.responseBody, ContentType.APPLICATION_JSON))
          response.setStatusCode(200)
        }
      }
      startServer(handler)

      val httpClient: CloseableHttpClient = HttpClients.createDefault()
      val oauthClient = new OAuthClient(httpClient, AuthConfig(),
        "http://localhost:1080/token", "client-id", "client-secret")

      val start = System.currentTimeMillis()
      val token = oauthClient.clientCredentials()
      val end = System.currentTimeMillis()

      assert(token.accessToken == testScenario.expectedAccessToken)
      assert(token.expiresIn == testScenario.expectedExpiresIn)
      assert(token.creationTimestamp >= start && token.creationTimestamp <= end)

      stopServer()
    }
  }

  test("OAuthClient should handle 401 Unauthorized response") {
    val handler = new HttpRequestHandler {
      @throws[HttpException]
      @throws[IOException]
      override def handle(request: HttpRequest,
                          response: HttpResponse,
                          context: HttpContext): Unit = {
        response.setStatusCode(401)
        response.setEntity(new StringEntity("Unauthorized", ContentType.TEXT_PLAIN))
      }
    }

    startServer(handler)

    val httpClient: CloseableHttpClient = HttpClients.createDefault()
    val oauthClient = new OAuthClient(httpClient, AuthConfig(),
      "http://localhost:1080/token", "client-id", "client-secret")

    val e = intercept[RuntimeException] {
      oauthClient.clientCredentials()
    }
    assert(e.getMessage.contains("Unauthorized"))

    stopServer()
  }

  override def afterEach(): Unit = {
    stopServer()
  }
}
