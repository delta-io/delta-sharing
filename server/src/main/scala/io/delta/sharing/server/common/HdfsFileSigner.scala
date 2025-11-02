/*
 * HDFS content-server signer: creates short-lived JWT for the content server
 */
package io.delta.sharing.server.common

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.{Duration, Instant}
import java.util

import org.apache.hadoop.fs.Path
import org.jose4j.jwa.AlgorithmConstraints
import org.jose4j.jwa.AlgorithmConstraints.ConstraintType
import org.jose4j.jws.AlgorithmIdentifiers
import org.jose4j.jws.JsonWebSignature
import org.jose4j.jwt.JwtClaims
import org.jose4j.keys.EdDsaKeyUtil
import io.delta.sharing.server.config.HdfsSignerConfig
import org.jose4j.jwt.NumericDate

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
  private val resolved: (String, java.security.PrivateKey, Option[String], Option[String]) = {
    HdfsFileSigner.config match {
      case Some(cfg) =>
        val pem = new String(Files.readAllBytes(Paths.get(cfg.getSigningPrivateKeyFile)), StandardCharsets.UTF_8)
        (cfg.getContentServerBase, new EdDsaKeyUtil().fromPemEncodedPrivateKey(pem), Option(cfg.getAudience), Option(cfg.getKid))
      case None =>
        val contentBase = propOrEnv("CONTENT_SERVER_BASE", "content.server.base").getOrElse("http://localhost:8443")
        val privateKeyPemPath = propOrEnv("SIGNING_PRIVATE_KEY", "signing.key.path")
          .getOrElse(throw new IllegalStateException("Missing signing key path: SIGNING_PRIVATE_KEY or -Dsigning.key.path"))
        val pem = new String(Files.readAllBytes(Paths.get(privateKeyPemPath)), StandardCharsets.UTF_8)
        val audience = propOrEnv("SIGNING_AUDIENCE", "signing.aud")
        val kid = propOrEnv("SIGNING_KID", "signing.kid")
        (contentBase, new EdDsaKeyUtil().fromPemEncodedPrivateKey(pem), audience, kid)
    }
  }

  override def sign(path: Path): PreSignedUrl = {
    val hdfsPath = Option(path).map(_.toUri.getPath).getOrElse("/")
    val now = Instant.now
    val exp = now.plusSeconds(preSignedUrlTimeoutSeconds)

    val claims = new JwtClaims()
    claims.setExpirationTimeNumericDate(NumericDate.fromSeconds(exp.getEpochSecond))
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
    val url = s"${resolved._1.stripSuffix("/")}/get?token=" + URLEncoder.encode(jwt, "UTF-8")
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
