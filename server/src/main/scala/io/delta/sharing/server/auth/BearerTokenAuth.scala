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

import java.util.Collections

import scala.collection.immutable.HashMap
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

import io.delta.sharing.server.config.ServerConfig
import io.delta.sharing.server.util.AuthUtils

class BearerTokenAuth(serverConfig: ServerConfig) extends AuthManager {
  // Keep track of all shares (by lowercase name) to make "universal bearer" returns faster
  private val shares: Seq[String] = serverConfig.getShares.asScala
    .map(_.getName.toLowerCase())
    .toSeq

  private val universalToken: String = serverConfig.getAuthorization.getUniversalBearerToken

  // Convert the auth shares list to map of token -> share set (if it exists)
  private val tokenToShares = Option(serverConfig.getAuthorization.getShares)
    .getOrElse(Collections.emptyList())
    .asScala
    // Convert to map of bearer token -> set of share names with that token
    .foldLeft(HashMap[String, Set[String]]())((m, s) => {
      val existingSet = m.getOrElse(s.getBearerToken, Set())
      m + (s.getBearerToken -> (existingSet + s.getName))
    })

  // The list of shares accessible without a valid bearer token. This only needs
  // to be populated when the "requireForAllShares" property is set to false
  private var unsecuredShares = Set[String]()
  if (!serverConfig.getAuthorization.getRequireForAllShares) {
    // Names of shares with explicit auth
    val securedShares = Option(serverConfig.getAuthorization.getShares)
      .getOrElse(Collections.emptyList())
      .asScala
      .map(_.getName.toLowerCase())
      .toSet

    // Subtract explicitly secured shares from all shares to get the list of unsecured shares
    unsecuredShares = shares.filter(s => !securedShares.contains(s)).toSet
  }

  private def hasUniversalAccess(bearerToken: Option[String]): Boolean = {
    if (universalToken == null) return false

    bearerToken.exists(bt =>
      AuthUtils.compareBearerTokens(bt, universalToken)
    )
  }

  override def hasShareAccess(bearerToken: Option[String], share: String): Boolean = {
    if (hasUniversalAccess(bearerToken)) return true

    if (unsecuredShares.contains(share.toLowerCase())) return true

    bearerToken.exists(bt =>
      tokenToShares.get(bt).exists(_.contains(share.toLowerCase()))
    )
  }

  override def getAuthorizedShares(bearerToken: Option[String]): Seq[String] = {
    if (hasUniversalAccess(bearerToken)) return shares

    val authorizedShares = bearerToken.map(bt =>
      tokenToShares.get(bt).map(_.toSeq).getOrElse(Seq())
    ).getOrElse(Seq())

    unsecuredShares.toSeq ++ authorizedShares
  }
}
