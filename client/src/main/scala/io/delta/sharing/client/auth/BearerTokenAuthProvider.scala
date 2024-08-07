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

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.ISO_DATE_TIME

import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpRequestBase

private[client] case class BearerTokenAuthProvider(bearerToken: String, expirationTime: String)
  extends AuthCredentialProvider {
  override def addAuthHeader(httpRequest: HttpRequestBase): Unit = {
    httpRequest.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $bearerToken")
  }

  override def isExpired(): Boolean = {
    if (expirationTime == null) return false
    try {
      val expirationTimeAsTimeStamp = Timestamp.valueOf(
        LocalDateTime.parse(expirationTime, ISO_DATE_TIME))
      expirationTimeAsTimeStamp.before(Timestamp.valueOf(LocalDateTime.now()))
    } catch {
      case _: Throwable => false
    }
  }

  override def getExpirationTime(): Option[String] = {
    expirationTime match {
      case null => None
      case _ => Some(expirationTime)
    }
  }
}
