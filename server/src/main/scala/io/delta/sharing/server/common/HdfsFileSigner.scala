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
// HDFS content-server signer: creates short-lived JWT for the content server
package io.delta.sharing.server.common

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.security.PrivateKey
import java.time.Instant

import org.apache.hadoop.fs.Path

import org.jose4j.jwa.AlgorithmConstraints
import org.jose4j.jwa.AlgorithmConstraints.ConstraintType
import org.jose4j.jws.AlgorithmIdentifiers
import org.jose4j.jws.JsonWebSignature
import org.jose4j.jwt.{JwtClaims, NumericDate}
import org.jose4j.keys.EdDsaKeyUtil

import io.delta.sharing.server.config.HdfsSignerConfig

/**
 * Produces PreSignedUrl values that point to a Content Server `/get?token=...` endpoint.
 *
 * Required configuration (env or system properties):
 *  - CONTENT_SERVER_BASE or -Dcontent.server.base (e.g., https://content.example.com)
 *  - SIGNING_PRIVATE_KEY or -Dsigning.key.path (PEM file path)
 * Optional:
 *  - SIGNING_AUDIENCE or -Dsigning.aud
 *  - SIGNING_KID or -Dsigning.kid
 */
class HdfsFileSigner(preSignedUrlTimeoutSeconds: Long) extends CloudFileSigner {
  private val resolved: (String, PrivateKey, Option[String], Option[String]) = {
    HdfsFileSigner.config match {
      case Some(cfg) =>
        val pem = new String(
          Files.readAllBytes(Paths.get(cfg.getSigningPrivateKeyFile)),
          StandardCharsets.UTF_8)
        val key = new EdDsaKeyUtil().fromPemEncoded(pem).asInstanceOf[PrivateKey]
        (cfg.getContentServerBase,
          key,
          Option(cfg.getAudience),
          Option(cfg.getKid))
      case None =>
        val contentBase = propOrEnv("CONTENT_SERVER_BASE", "content.server.base")
          .getOrElse("http://localhost:8443")
        val privateKeyPemPath = propOrEnv("SIGNING_PRIVATE_KEY", "signing.key.path")
          .getOrElse(throw new IllegalStateException(
            "Missing signing key path: SIGNING_PRIVATE_KEY or -Dsigning.key.path"))
        val pem = new String(
          Files.readAllBytes(Paths.get(privateKeyPemPath)),
          StandardCharsets.UTF_8)
        val audience = propOrEnv("SIGNING_AUDIENCE", "signing.aud")
        val kid = propOrEnv("SIGNING_KID", "signing.kid")
        val key = new EdDsaKeyUtil().fromPemEncoded(pem).asInstanceOf[PrivateKey]
        (contentBase, key, audience, kid)
    }
  }

  override def sign(path: Path): PreSignedUrl = {
    val hdfsPath = Option(path).map(_.toUri.getPath).getOrElse("/")
    val now = Instant.now
    val exp = now.plusSeconds(preSignedUrlTimeoutSeconds)

    val claims = new JwtClaims()
    claims.setExpirationTime(NumericDate.fromSeconds(exp.getEpochSecond))
    resolved._3.foreach(claims.setAudience)
    claims.setGeneratedJwtId()
    claims.setClaim("hdfs_path", hdfsPath)

    val jws = new JsonWebSignature()
    jws.setPayload(claims.toJson)
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.EDDSA)
    resolved._4.foreach(jws.setKeyIdHeaderValue)
    jws.setKey(resolved._2)
    jws.setHeader("typ", "JWT")
    jws.setDoKeyValidation(false)
    jws.setAlgorithmConstraints(new AlgorithmConstraints(ConstraintType.WHITELIST, AlgorithmIdentifiers.EDDSA))

    val jwt = jws.getCompactSerialization
    val url = s"${resolved._1.stripSuffix("/")}/get?token=" +
      URLEncoder.encode(jwt, "UTF-8")
    new PreSignedUrl(url, System.currentTimeMillis() + preSignedUrlTimeoutSeconds * 1000L)
  }

  private def propOrEnv(env: String, prop: String): Option[String] = {
    Option(System.getProperty(prop)).filter(_.nonEmpty).orElse(Option(System.getenv(env)).filter(_.nonEmpty))
  }
}

object HdfsFileSigner {
  @volatile private var _config: Option[HdfsSignerConfig] = None
  def configureFrom(conf: HdfsSignerConfig): Unit = { _config = Option(conf) }
  def config: Option[HdfsSignerConfig] = _config
}
