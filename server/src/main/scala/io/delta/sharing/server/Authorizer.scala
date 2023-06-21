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

package io.delta.sharing.server

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.linecorp.armeria.common.auth.OAuth2Token
import com.linecorp.armeria.server.ServiceRequestContext
import com.linecorp.armeria.server.auth.Authorizer

import io.delta.sharing.server.config.ServerConfig

class SimpleAuthorizer(serverConfig: ServerConfig) extends Authorizer[OAuth2Token] {
  override def authorize(ctx: ServiceRequestContext, token: OAuth2Token): CompletionStage[java.lang.Boolean] = {
    // Use `MessageDigest.isEqual` to do a time-constant comparison to avoid timing attacks
    val authorized = MessageDigest.isEqual(
      token.accessToken.getBytes(UTF_8),
      serverConfig.getAuthorization.getBearerToken.getBytes(UTF_8))
    CompletableFuture.completedFuture(authorized)
  }
}

class ManagementDbAuthorizer(serverConfig: ServerConfig) extends Authorizer[OAuth2Token] {
  override def authorize(ctx: ServiceRequestContext, data: OAuth2Token): CompletionStage[java.lang.Boolean] = {
    CompletableFuture.completedFuture(true)
  }
}
