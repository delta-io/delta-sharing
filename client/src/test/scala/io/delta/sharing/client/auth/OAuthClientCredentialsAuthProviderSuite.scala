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
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.CloseableHttpClient
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import io.delta.sharing.client.OAuthClientCredentialsDeltaSharingProfile

class OAuthClientCredentialsAuthProviderSuite extends AnyFunSuite with MockitoSugar {

  test("OAuthClientCredentialsAuthProvider should exchange clientId and clientSecret" +
    " for an access token on the first request") {
    val client = mock[CloseableHttpClient]
    val mockOauthClient = mock[OAuthClient]
    val profile = OAuthClientCredentialsDeltaSharingProfile(
      Some(2),
      "http://example.com/delta-sharing", "http://example.com/token", "client-id", "client-secret"
    )
    val provider = new OAuthClientCredentialsAuthProvider(client, AuthConfig(), profile) {
      override lazy val oauthClient: OAuthClient = mockOauthClient
    }
    val request = new HttpGet("http://example.com")

    // Mock the token refresh
    val token = OAuthClientCredentials("access-token",
      3600, System.currentTimeMillis())
    when(mockOauthClient.clientCredentials()).thenReturn(token)

    provider.addAuthHeader(request)

    assert(request.getFirstHeader(HttpHeaders.AUTHORIZATION)
      .getValue == s"Bearer ${token.accessToken}")
    verify(mockOauthClient, times(1)).clientCredentials()
  }

  test("OAuthClientCredentialsAuthProvider should re-use current token" +
    " if valid for another 11 minutes") {
    val client = mock[CloseableHttpClient]
    val mockOauthClient = mock[OAuthClient]
    val profile = OAuthClientCredentialsDeltaSharingProfile(
      Some(2),
      "http://example.com/delta-sharing", "http://example.com/token", "client-id", "client-secret"
    )
    val authConfig = AuthConfig()
    val provider = new OAuthClientCredentialsAuthProvider(client, authConfig, profile) {
      override lazy val oauthClient: OAuthClient = mockOauthClient
    }
    val request = new HttpGet("http://example.com")

    // Set a token that is valid for another 11 minutes
    val validToken = OAuthClientCredentials("valid-token",
      authConfig.tokenRenewalThresholdInSeconds + 1, System.currentTimeMillis())
    provider.setCurrentTokenForTesting(validToken)

    provider.addAuthHeader(request)

    // Verify that the token is not refreshed
    verify(mockOauthClient, never()).clientCredentials()
    assert(request.getFirstHeader(HttpHeaders.AUTHORIZATION)
      .getValue == s"Bearer ${validToken.accessToken}")
  }

  test("OAuthClientCredentialsAuthProvider should refresh token if expired") {
    val client = mock[CloseableHttpClient]
    val mockOauthClient = mock[OAuthClient]
    val profile = OAuthClientCredentialsDeltaSharingProfile(
      Some(2),
      "http://example.com/delta-sharing", "http://example.com/token", "client-id", "client-secret"
    )
    val authConfig = AuthConfig()

    val provider = new OAuthClientCredentialsAuthProvider(client, authConfig, profile) {
      override lazy val oauthClient: OAuthClient = mockOauthClient
    }
    val request = new HttpGet("http://example.com")

    // Mock the token refresh
    val expiredToken = OAuthClientCredentials("expired-token",
      1, System.currentTimeMillis() - 3600 * 1000)
    val newToken = OAuthClientCredentials("new-token", 3600, System.currentTimeMillis())
    when(mockOauthClient.clientCredentials()).thenReturn(newToken)

    // Set the expired token
    provider.setCurrentTokenForTesting(expiredToken)

    provider.addAuthHeader(request)

    assert(request.getFirstHeader(HttpHeaders.AUTHORIZATION)
      .getValue == s"Bearer ${newToken.accessToken}")
    verify(mockOauthClient, times(1)).clientCredentials()
  }


  test("needsRefresh should return true if the token is expired") {
    val client = mock[CloseableHttpClient]
    val profile = OAuthClientCredentialsDeltaSharingProfile(
      Some(2),
      "http://example.com/delta-sharing", "http://example.com/token", "client-id", "client-secret"
    )

    val provider = OAuthClientCredentialsAuthProvider(client, AuthConfig(), profile)

    val expiredToken = OAuthClientCredentials("expired-token",
      1, System.currentTimeMillis() - 3600 * 1000)

    assert(provider.needsRefresh(expiredToken))
  }

  test("needsRefresh should return true if the token expires in less than 10 minutes") {
    val client = mock[CloseableHttpClient]
    val profile = OAuthClientCredentialsDeltaSharingProfile(
      Some(2),
      "http://example.com/delta-sharing", "http://example.com/token", "client-id", "client-secret"
    )

    val authConfig = AuthConfig()
    val provider = OAuthClientCredentialsAuthProvider(client, authConfig, profile)

    val tokenExpiringSoon = OAuthClientCredentials("expiring-soon-token",
      authConfig.tokenRenewalThresholdInSeconds - 5, System.currentTimeMillis())

    assert(provider.needsRefresh(tokenExpiringSoon))
  }

  test("needsRefresh should return false if the token is valid for more than 10 minutes") {
    val client = mock[CloseableHttpClient]
    val profile = OAuthClientCredentialsDeltaSharingProfile(
      Some(2),
      "http://example.com/delta-sharing", "http://example.com/token", "client-id", "client-secret"
    )
    val authConfig = AuthConfig()
    val provider = OAuthClientCredentialsAuthProvider(client, authConfig, profile)

    val validToken = OAuthClientCredentials("valid-token",
      authConfig.tokenRenewalThresholdInSeconds + 10, System.currentTimeMillis())

    assert(!provider.needsRefresh(validToken))
  }



  test("OAuthClientCredentialsAuthProvider isExpired always is false") {
    val client = mock[CloseableHttpClient]
    val profile = OAuthClientCredentialsDeltaSharingProfile(
      Some(2),
      "http://example.com/delta-sharing", "http://example.com/token", "client-id", "client-secret"
    )
    val authConfig = AuthConfig()
    val provider = OAuthClientCredentialsAuthProvider(client, authConfig, profile)

    val validToken = OAuthClientCredentials("valid-token", 3600, System.currentTimeMillis())
    provider.setCurrentTokenForTesting(validToken)

    assert(!provider.isExpired)
  }

  test("OAuthClientCredentialsAuthProvider getExpirationTime should return None") {
    val client = mock[CloseableHttpClient]
    val profile = OAuthClientCredentialsDeltaSharingProfile(
      Some(2),
      "http://example.com/delta-sharing", "http://example.com/token", "client-id", "client-secret"
    )
    val authConfig = AuthConfig()

    val provider = OAuthClientCredentialsAuthProvider(client, authConfig, profile)

    assert(provider.getExpirationTime.isEmpty)
  }
}
