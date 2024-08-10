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
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class BearerTokenAuthProviderSuite extends AnyFunSuite with MockitoSugar {

  test("BearerTokenAuthProvider should add Authorization header") {
    val bearerToken = "test-token"
    val provider = BearerTokenAuthProvider(bearerToken, null)
    val request = new HttpGet("http://example.com")

    provider.addAuthHeader(request)

    assert(request.getFirstHeader(HttpHeaders.AUTHORIZATION)
      .getValue == s"Bearer $bearerToken")
  }

  test("BearerTokenAuthProvider should correctly identify expired token") {
    val expiredToken = "expired-token"
    val expirationTime = "2020-01-01T00:00:00.0Z"
    val provider = BearerTokenAuthProvider(expiredToken, expirationTime)

    assert(provider.isExpired())
  }

  test("BearerTokenAuthProvider should correctly identify non-expired token") {
    val validToken = "valid-token"
    val expirationTime = "2999-01-01T00:00:00.0Z"
    val provider = BearerTokenAuthProvider(validToken, expirationTime)

    assert(!provider.isExpired())
  }

  test("BearerTokenAuthProvider should return correct expiration time") {
    val token = "test-token"
    val expirationTime = "2021-11-12T00:12:29.0Z"
    val provider = BearerTokenAuthProvider(token, expirationTime)

    assert(provider.getExpirationTime().contains(expirationTime))
  }

  test("BearerTokenAuthProvider should return None for null expiration time") {
    val token = "test-token"
    val provider = BearerTokenAuthProvider(token, null)

    assert(provider.getExpirationTime().isEmpty)
  }
}
