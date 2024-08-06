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

import java.util.Base64

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.BaseJsonNode
import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.util.EntityUtils

import io.delta.sharing.client.util.JsonUtils

case class OAuthClientCredentials(accessToken: String,
                                  expiresIn: Long,
                                  creationTimestamp: Long)

private[client] class OAuthClient(httpClient:
                                  CloseableHttpClient,
                                  tokenEndpoint: String,
                                  clientId: String,
                                  clientSecret: String,
                                  scope: Option[String] = None) {

  def clientCredentials(): OAuthClientCredentials = {
    val credentials = Base64.getEncoder.encodeToString(s"$clientId:$clientSecret".getBytes("UTF-8"))

    val post = new HttpPost(tokenEndpoint)
    post.setHeader("accept", "application/json")
    post.setHeader("authorization", s"Basic $credentials")
    post.setHeader("content-type", "application/x-www-form-urlencoded")

    val scopeParam = scope.map(s => s"&scope=$s").getOrElse("")
    val body = s"grant_type=client_credentials$scopeParam"
    post.setEntity(new StringEntity(body))

    var response: CloseableHttpResponse = null
    try {
      response = httpClient.execute(post)
      val responseString = getResponseAsString(response.getEntity)
      if (response.getStatusLine.getStatusCode != 200 || responseString == null) {
        throw new RuntimeException(s"Failed to get OAuth token from token endpoint: " +
          s"Token Endpoint responded: ${response.getStatusLine} with response: $responseString")
      }

      parseOAuthTokenResponse(responseString)

    } finally {
      if (response != null) response.close()
    }
  }

  private def parseOAuthTokenResponse(response: String): OAuthClientCredentials = {
    if (response == null || response.isEmpty) {
      throw new RuntimeException("Empty response from OAuth token endpoint")
    }
    val jsonNode = JsonUtils.readTree(response)
    if (!jsonNode.has("access_token") || !jsonNode.get("access_token").isTextual) {
      throw new RuntimeException("Missing 'access_token' field in OAuth token response")
    }
    if (!jsonNode.has("expires_in") || !jsonNode.get("expires_in").isNumber) {
      throw new RuntimeException("Missing 'expires_in' field in OAuth token response")
    }

    OAuthClientCredentials(
      jsonNode.get("access_token").asText(),
      jsonNode.get("expires_in").asLong(),
      System.currentTimeMillis()
    )
  }

  private def getResponseAsString(httpEntity: HttpEntity) : String = {
    if (httpEntity != null) {
      EntityUtils.toString(httpEntity)
    } else {
      null
    }
  }
}
