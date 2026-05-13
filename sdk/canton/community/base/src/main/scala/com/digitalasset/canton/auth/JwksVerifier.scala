// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.auth0.jwk.{JwkException, UrlJwkProvider}
import com.auth0.jwt.algorithms.Algorithm
import com.daml.jwt.{
  DecodedJwt,
  ECDSAVerifier,
  Error,
  Jwt,
  JwtTimestampLeeway,
  JwtVerifier,
  JwtVerifierBase,
  RSA256Verifier,
  WithExecuteUnsafe,
}
import com.google.common.cache.{Cache, CacheBuilder}

import java.net.{URI, URL}
import java.security.interfaces.{ECPublicKey, RSAPublicKey}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/** A JWK verifier, where the public keys are automatically fetched from the given JWKS URL.
  *
  * In JWKS, each key ID uniquely identifies a public key. The keys are kept in cache, in order to
  * prevent having to do a remote network access for each token validation.
  *
  * The cache is limited both in size and time. A size limit protects against infinitely growing
  * memory consumption. A time limit is a safety catch for the case where a public key is used to
  * sign a token without an expiration time and then is revoked.
  *
  * @param url
  *   The URL that points to the JWKS JSON document
  * @param cacheMaxSize
  *   Maximum number of public keys to keep in the cache.
  * @param cacheExpirationTime
  *   Maximum time to keep public keys in the cache.
  * @param connectionTimeout
  *   Timeout for connecting to the JWKS URL.
  * @param readTimeout
  *   Timeout for reading from the JWKS URL.
  */
class JwksVerifier(
    url: URL,
    // Large enough such that malicious users can't cycle through all keys from reasonably sized JWKS,
    // forcing cache eviction and thus introducing additional latency.
    cacheMaxSize: Long,
    cacheExpiration: FiniteDuration,
    connectionTimeout: FiniteDuration,
    readTimeout: FiniteDuration,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    maxTokenLife: Option[Long] = None,
) extends JwtVerifierBase
    with WithExecuteUnsafe {

  private[this] val http =
    new UrlJwkProvider(
      url,
      connectionTimeout.toMillis.toInt,
      readTimeout.toMillis.toInt,
    )

  private[this] val cache: Cache[String, JwtVerifier] = CacheBuilder
    .newBuilder()
    .maximumSize(cacheMaxSize)
    .expireAfterWrite(cacheExpiration.toSeconds, TimeUnit.SECONDS)
    .build()

  @SuppressWarnings(
    Array("org.wartremover.warts.Null")
  )
  private[this] def getVerifier(keyId: String): Either[Error, JwtVerifier] =
    try {
      val jwk = http.get(keyId)
      val publicKey = jwk.getPublicKey
      publicKey match {
        case rsa: RSAPublicKey => RSA256Verifier(rsa, jwtTimestampLeeway, maxTokenLife)
        case ec: ECPublicKey if ec.getParams.getCurve.getField.getFieldSize == 256 =>
          ECDSAVerifier(Algorithm.ECDSA256(ec, null), jwtTimestampLeeway, maxTokenLife)
        case ec: ECPublicKey if ec.getParams.getCurve.getField.getFieldSize == 521 =>
          ECDSAVerifier(Algorithm.ECDSA512(ec, null), jwtTimestampLeeway, maxTokenLife)
        case key =>
          Left(
            Error(
              Symbol("JwksVerifier.getVerifier"),
              s"Unsupported public key format ${key.getFormat}",
            )
          )
      }
    } catch {
      case e: JwkException =>
        Left(Error(Symbol("JwksVerifier.getVerifier"), s"Couldn't get jwk from http: $e"))
      case _: Throwable =>
        Left(
          Error(Symbol("JwksVerifier.getVerifier"), s"Unknown error while getting jwk from http")
        )
    }

  /** Looks up the verifier for the given keyId from the local cache. On a cache miss, creates a new
    * verifier by fetching the public key from the JWKS URL.
    */
  private[this] def getCachedVerifier(keyId: String): Either[Error, JwtVerifier] =
    if (keyId == null)
      Left(Error(Symbol("JwksVerifier.getCachedVerifier"), "No Key ID found"))
    else
      executeUnsafe(
        cache.get(keyId, () => getVerifier(keyId).fold(e => sys.error(e.prettyPrint), x => x)),
        Symbol("JwksVerifier.getCachedVerifier"),
      )

  def verify(jwt: Jwt): Either[Error, DecodedJwt[String]] =
    for {
      keyId <- executeUnsafe(
        com.auth0.jwt.JWT.decode(jwt.value).getKeyId,
        Symbol("JwksVerifier.verify"),
      )
      verifier <- getCachedVerifier(keyId)
      decoded <- verifier.verify(jwt)
    } yield decoded
}

object JwksVerifier {
  def apply(
      url: String,
      cacheMaxSize: Long,
      cacheExpiration: FiniteDuration,
      connectionTimeout: FiniteDuration,
      readTimeout: FiniteDuration,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
      maxTokenLife: Option[Long] = None,
  ) =
    new JwksVerifier(
      new URI(url).toURL,
      cacheMaxSize = cacheMaxSize,
      cacheExpiration = cacheExpiration,
      connectionTimeout = connectionTimeout,
      readTimeout = readTimeout,
      jwtTimestampLeeway = jwtTimestampLeeway,
      maxTokenLife = maxTokenLife,
    )
}
