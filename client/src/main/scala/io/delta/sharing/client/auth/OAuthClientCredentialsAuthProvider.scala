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

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.impl.client.CloseableHttpClient

import io.delta.sharing.client.OAuthClientCredentialsDeltaSharingProfile

private[client] case class OAuthClientCredentialsAuthProvider(
  client: CloseableHttpClient,
  authConfig: AuthConfig,
  profile: OAuthClientCredentialsDeltaSharingProfile) extends AuthCredentialProvider {

  private val readWriteLock = new ReentrantReadWriteLock()
  private val readLock = readWriteLock.readLock
  private val writeLock = readWriteLock.writeLock

  private[auth] lazy val oauthClient = new OAuthClient(client, authConfig,
    profile.tokenEndpoint, profile.clientId, profile.clientSecret, profile.scope)

  // this can be updated on different thread
  // read has be through readLock and write has to be through writeLock
  private var currentToken: Option[OAuthClientCredentials] = None

  override def addAuthHeader(httpRequest: HttpRequestBase): Unit = {
    val token = maybeRefreshToken()

    readLock.lock()
    try {
      httpRequest.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer ${token.accessToken}")
    } finally {
      readLock.unlock()
    }
  }

  // Method to set the current token for testing purposes
  private[auth] def setCurrentTokenForTesting(token: OAuthClientCredentials): Unit = {
    writeLock.lock()
    try {
      currentToken = Some(token)
    } finally {
      writeLock.unlock()
    }
  }

  private def maybeRefreshToken(): OAuthClientCredentials = {
    readLock.lock()
    try {
      if (currentToken.isDefined && !needsRefresh(currentToken.get)) {
        return currentToken.get
      }
    } finally {
      readLock.unlock()
    }

    writeLock.lock()
    try {
      if (currentToken.isEmpty || needsRefresh(currentToken.get)) {
        val newToken = oauthClient.clientCredentials()
        currentToken = Some(newToken)
      }

      currentToken.get
    } finally {
      writeLock.unlock()
    }
  }

  private[auth] def needsRefresh(token: OAuthClientCredentials): Boolean = {
    val now = System.currentTimeMillis()
    val expirationTime = token.creationTimestamp + token.expiresIn * 1000
    expirationTime - now < authConfig.tokenRenewalThresholdInSeconds * 1000
  }

  override def getExpirationTime(): Option[String] = None
}
