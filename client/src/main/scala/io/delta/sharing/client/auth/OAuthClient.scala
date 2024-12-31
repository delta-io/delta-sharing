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

import io.delta.sharing.client.util.{JsonUtils, RetryUtils, UnexpectedHttpStatus}

case class OAuthClientCredentials(accessToken: String,
                                  expiresIn: Long,
                                  creationTimestamp: Long)

private[client] class OAuthClient(httpClient:
                                  CloseableHttpClient,
                                  authConfig: AuthConfig,
                                  tokenEndpoint: String,
                                  clientId: String,
                                  clientSecret: String,
                                  scope: Option[String] = None) {

  def clientCredentials(): OAuthClientCredentials = {

    // see client credentials grant spec detail here:
    // https://www.oauth.com/oauth2-servers/access-tokens/client-credentials/
    // https://datatracker.ietf.org/doc/html/rfc6749
    val credentials = Base64.getEncoder.encodeToString(s"$clientId:$clientSecret".getBytes("UTF-8"))

    val post = new HttpPost(tokenEndpoint)
    post.setHeader("accept", "application/json")
    post.setHeader("authorization", s"Basic $credentials")
    post.setHeader("content-type", "application/x-www-form-urlencoded")

    val scopeParam = scope.map(s => s"&scope=$s").getOrElse("")
    val body = s"grant_type=client_credentials$scopeParam"
    post.setEntity(new StringEntity(body))

    // retries on temporary errors (connection error or 500, 429) from token endpoint
    RetryUtils.runWithExponentialBackoff(
      authConfig.tokenExchangeMaxRetries,
      authConfig.tokenExchangeMaxRetryDurationInSeconds * 1000) {
      var response: CloseableHttpResponse = null
      try {
        response = httpClient.execute(post)
        val responseString = getResponseAsString(response.getEntity)
        if (response.getStatusLine.getStatusCode != 200) {
          throw new UnexpectedHttpStatus(s"Failed to get OAuth token from token endpoint: " +
            s"Token Endpoint responded: ${response.getStatusLine} with response: $responseString",
            response.getStatusLine.getStatusCode)
        }

        parseOAuthTokenResponse(responseString)
      } finally {
        if (response != null) response.close()
      }
    }
  }

  private def parseOAuthTokenResponse(response: String): OAuthClientCredentials = {
    // Parsing the response per oauth spec
    // https://datatracker.ietf.org/doc/html/rfc6749#section-5.1
    if (response == null || response.isEmpty) {
      throw new RuntimeException("Empty response from OAuth token endpoint")
    }
    val jsonNode = JsonUtils.readTree(response)
    if (!jsonNode.has("access_token") || !jsonNode.get("access_token").isTextual) {
      throw new RuntimeException("Missing 'access_token' field in OAuth token response")
    }
    if (!jsonNode.has("expires_in")) {
      throw new RuntimeException("Missing 'expires_in' field in OAuth token response")
    }

    // OAuth spec requires 'expires_in' to be an integer, e.g., 3600.
    // See https://datatracker.ietf.org/doc/html/rfc6749#section-5.1
    // But some token endpoints return `expires_in` as a string e.g., "3600".
    // This ensures that we support both integer and string values for 'expires_in' field.
    // Example request resulting in 'expires_in' as a string:
    // curl -X POST \
    //  https://login.windows.net/$TENANT_ID/oauth2/token \
    //  -H "Content-Type: application/x-www-form-urlencoded" \
    //  -d "grant_type=client_credentials" \
    //  -d "client_id=$CLIENT_ID" \
    //  -d "client_secret=$CLIENT_SECRET" \
    //  -d "scope=https://graph.microsoft.com/.default"
    val expiresIn : Long = jsonNode.get("expires_in") match {
      case n if n.isNumber => n.asLong()
      case n if n.isTextual =>
        try {
          n.asText().toLong
        } catch {
          case _: NumberFormatException =>
            throw new RuntimeException("Invalid 'expires_in' field in OAuth token response")
        }
      case _ =>
        throw new RuntimeException("Invalid 'expires_in' field in OAuth token response")
    }

    OAuthClientCredentials(
      jsonNode.get("access_token").asText(),
      expiresIn,
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

