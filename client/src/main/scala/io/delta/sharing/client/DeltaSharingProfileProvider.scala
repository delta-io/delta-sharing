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

package io.delta.sharing.client

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.delta.sharing.TableRefreshResult

import io.delta.sharing.client.DeltaSharingProfile.{validateNotNullAndEmpty, BEARER_TOKEN,
  OAUTH_CLIENT_CREDENTIALS}
import io.delta.sharing.client.util.JsonUtils

@JsonDeserialize(using = classOf[DeltaSharingProfileDeserializer])
sealed trait DeltaSharingProfile {
  val shareCredentialsVersion: Option[Int]
  val endpoint: String
  val profileType : String

  private [client] def validate(): Unit = {
    if (shareCredentialsVersion.isEmpty) {
      throw new IllegalArgumentException(
        "Cannot find the 'shareCredentialsVersion' field in the profile file")
    }

    if (shareCredentialsVersion.get > DeltaSharingProfile.CURRENT) {
      throw new IllegalArgumentException(
        s"'shareCredentialsVersion' in the profile is " +
          s"${shareCredentialsVersion.get} which is too new. The current release " +
          s"supports version ${DeltaSharingProfile.CURRENT} and below. Please upgrade to a newer " +
          s"release.")
    }

    validateNotNullAndEmpty(endpoint, "endpoint")
  }
}

case class BearerTokenDeltaSharingProfile(
    override val shareCredentialsVersion: Option[Int],
    override val endpoint: String = null,
    bearerToken: String = null,
    expirationTime: String = null)
  extends DeltaSharingProfile {
  override val profileType: String = BEARER_TOKEN

  super.validate()
  if (shareCredentialsVersion.get != 1) {
    throw new IllegalArgumentException(
      s"${profileType} only supports version 1")
  }
  DeltaSharingProfile.validateNotNullAndEmpty(bearerToken, "bearerToken")
}

case class OAuthClientCredentialsDeltaSharingProfile(
    override val shareCredentialsVersion: Option[Int],
    override val endpoint: String = null,
    tokenEndpoint: String = null,
    clientId: String = null,
    clientSecret: String = null,
    scope: Option[String] = None) extends DeltaSharingProfile {

  override val profileType: String = OAUTH_CLIENT_CREDENTIALS

  super.validate()
  if (shareCredentialsVersion.get != 2) {
    throw new IllegalArgumentException(
      s"$profileType only supports version 2")
  }
  DeltaSharingProfile.validateNotNullAndEmpty(tokenEndpoint, "tokenEndpoint")
  DeltaSharingProfile.validateNotNullAndEmpty(clientId, "clientId")
  DeltaSharingProfile.validateNotNullAndEmpty(clientSecret, "clientSecret")
}

private[client]
class DeltaSharingProfileDeserializer extends JsonDeserializer[DeltaSharingProfile] {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): DeltaSharingProfile = {
    val node: ObjectNode = p.getCodec.readTree(p).asInstanceOf[ObjectNode]
    val profileType = node.path("type").asText("bearer_token").toLowerCase(Locale.ROOT)
    profileType match {
      case "bearer_token" =>
        BearerTokenDeltaSharingProfile(
          shareCredentialsVersion = Option(node.get("shareCredentialsVersion")).map(_.asInt()),
          endpoint = Option(node.get("endpoint")).map(_.asText()).orNull,
          bearerToken = Option(node.get("bearerToken")).map(_.asText()).orNull,
          expirationTime = Option(node.get("expirationTime")).map(_.asText()).orNull
        )
      case "oauth_client_credentials" =>
        OAuthClientCredentialsDeltaSharingProfile(
          shareCredentialsVersion = Option(node.get("shareCredentialsVersion")).map(_.asInt()),
          endpoint = Option(node.get("endpoint")).map(_.asText()).orNull,
          tokenEndpoint = Option(node.get("tokenEndpoint")).map(_.asText()).orNull,
          clientId = Option(node.get("clientId")).map(_.asText()).orNull,
          clientSecret = Option(node.get("clientSecret")).map(_.asText()).orNull,
          scope = Option(node.get("scope")).map(_.asText())
        )
      case _ => throw new IllegalArgumentException(s"Unknown profile type $profileType")
    }
  }
}

object DeltaSharingProfile {
  val CURRENT = 2

  /**
   * Constructor to ensure backward compatibility for DeltaSharingProfile instantiation
   *
   * Note that the preferred way of creating a profile is using the concrete case classes.
   * Use the following case classes directly:
   * BearerTokenDeltaSharingProfile or OAuthClientCredentialsDeltaSharingProfile
   */
  def apply(shareCredentialsVersion: Some[Int],
            endpoint: String,
            bearerToken: String,
            expirationTime: String = null): DeltaSharingProfile = {
    BearerTokenDeltaSharingProfile(shareCredentialsVersion = shareCredentialsVersion,
      endpoint = endpoint, bearerToken = bearerToken, expirationTime = expirationTime)
  }

  private [client] val BEARER_TOKEN: String = "bearer_token"
  private [client] val OAUTH_CLIENT_CREDENTIALS: String = "oauth_client_credentials"

  private [client] def validateNotNullAndEmpty(fieldValue: String,
                                               fieldName: String): Unit = {
    if (fieldValue == null || fieldValue.isEmpty) {
      throw new IllegalArgumentException(s"Cannot find the '$fieldName' field in the profile file")
    }
  }

  private [client] def validateNotNullAndEmpty(fieldValue: Option[Long],
                                               fieldName: String): Unit = {
    if (fieldValue == null || fieldValue.isEmpty) {
      throw new IllegalArgumentException(s"Cannot find the '$fieldName' field in the profile file")
    }
  }
}

/**
 * A provider that provides Delta Sharing profile for data recipient to access the shared data.
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#profile-file-format.
 */
trait DeltaSharingProfileProvider {
  def getProfile: DeltaSharingProfile

  // A set of custom HTTP headers to get included in the HTTP requests sent to the delta sharing
  // server. This can be used to add extra information to the requests.
  def getCustomHeaders: Map[String, String] = Map.empty

  def getCustomTablePath(tablePath: String): String = tablePath

  // `refresher` takes an optional refreshToken, and returns
  // (idToUrlMap, minUrlExpirationTimestamp, refreshToken)
  def getCustomRefresher(
      refresher: Option[String] => TableRefreshResult): Option[String] => TableRefreshResult = {
    refresher
  }
}

/**
 * Load [[DeltaSharingProfile]] from a file. `conf` should be provided to load the file from remote
 * file systems.
 */
private[sharing] class DeltaSharingFileProfileProvider(
    conf: Configuration,
    file: String) extends DeltaSharingProfileProvider {

  val profile = {
    val input = new Path(file).getFileSystem(conf).open(new Path(file))
    val profile = try {
      JsonUtils.fromJson[DeltaSharingProfile](IOUtils.toString(input, UTF_8))
    } finally {
      input.close()
    }

    profile.validate()

    profile
  }

  override def getProfile: DeltaSharingProfile = profile
}
