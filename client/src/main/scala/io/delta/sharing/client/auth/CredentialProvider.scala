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

import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.impl.client.CloseableHttpClient

import io.delta.sharing.client.{BearerTokenDeltaSharingProfile, DeltaSharingProfile, OAuthClientCredentialsDeltaSharingProfile}

private[client] object AuthCredentialProviderFactory {
  def createCredentialProvider(profile: DeltaSharingProfile,
                               client: CloseableHttpClient): CredentialProvider = {
    profile match {
      case oauthProfile: OAuthClientCredentialsDeltaSharingProfile =>
        OAuthClientCredentialsAuthProvider(client, oauthProfile)
      case BearerTokenDeltaSharingProfile(_, _, bearerToken, _) =>
        BearerTokenAuthProvider(bearerToken)
    }
  }
}

private[client] trait CredentialProvider {
  def addAuthHeader(httpRequest: HttpRequestBase): Unit
}

private[client] case class BearerTokenAuthProvider(bearerToken: String) extends CredentialProvider {
  override def addAuthHeader(httpRequest: HttpRequestBase): Unit = {
    httpRequest.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $bearerToken")
  }
}

private[client] case class
OAuthClientCredentialsAuthProvider(client: CloseableHttpClient,
                                   profile: OAuthClientCredentialsDeltaSharingProfile)
  extends CredentialProvider {

  private[auth] lazy val oauthClient = new OAuthClient(client,
    profile.tokenEndpoint, profile.clientId, profile.clientSecret, profile.scope)

  private var currentToken: Option[OAuthClientCredentials] = None

  override def addAuthHeader(httpRequest: HttpRequestBase): Unit = {
    if (currentToken.isEmpty || needsRefresh(currentToken.get)) {
      refreshToken()
    }

    // assert currentToken is not empty
    httpRequest.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer ${currentToken.get.accessToken}")
  }

  // Method to set the current token for testing purposes
  private[auth] def setCurrentToken(token: OAuthClientCredentials): Unit = {
    synchronized {
      currentToken = Some(token)
    }
  }

  private def refreshToken(): Unit = {
    synchronized {
      if (currentToken.isEmpty || needsRefresh(currentToken.get)) {
        val newToken = oauthClient.clientCredentials()
        currentToken = Some(newToken)
      }
    }
  }

  private[auth] def needsRefresh(token: OAuthClientCredentials): Boolean = {
    val now = System.currentTimeMillis()
    val expirationTime = token.creationTimestamp + token.expiresIn * 1000
    expirationTime - now < OAuthClientCredentialsAuthProvider.expirationThresholdInSeconds * 1000
  }
}

private[client] object OAuthClientCredentialsAuthProvider {
  // We will refresh the token if it expires in less than 10 minutes.
  private val expirationThresholdInSeconds = 60 * 10 // 10 minutes
}
