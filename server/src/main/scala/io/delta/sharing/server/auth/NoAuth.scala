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

package io.delta.sharing.server.auth

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

import io.delta.sharing.server.config.ServerConfig

class NoAuth(serverConfig: ServerConfig) extends AuthManager {
  private val shares: Seq[String] = serverConfig.getShares.asScala
    .map(_.getName.toLowerCase())
    .toSeq

  override def hasShareAccess(bearerToken: Option[String], share: String): Boolean = true

  override def getAuthorizedShares(bearerToken: Option[String]): Seq[String] = shares
}
