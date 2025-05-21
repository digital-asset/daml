// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import com.auth0.jwk.{JwkException, UrlJwkProvider}
import com.auth0.jwt.algorithms.Algorithm
import com.google.common.cache.{Cache, CacheBuilder}
import scalaz.syntax.show.*
import scalaz.{-\/, Show, \/}

import java.net.{URI, URL}
import java.security.interfaces.{ECPublicKey, RSAPublicKey}
import java.util.concurrent.TimeUnit

/** A JWK verifier, where the public keys are automatically fetched from the given JWKS URL.
  *
  * In JWKS, each key ID uniquely identifies a public key.
  * The keys are kept in cache, in order to prevent having to do a remote network access for each token validation.
  *
  * The cache is limited both in size and time.
  * A size limit protects against infinitely growing memory consumption.
  * A time limit is a safety catch for the case where a public key is used to sign a token without an expiration time
  * and then is revoked.
  *
  * @param url The URL that points to the JWKS JSON document
  * @param cacheMaxSize Maximum number of public keys to keep in the cache.
  * @param cacheExpirationTime Maximum time to keep public keys in the cache.
  * @param connectionTimeout Timeout for connecting to the JWKS URL.
  * @param readTimeout Timeout for reading from the JWKS URL.
  */
class JwksVerifier(
    url: URL,
    // Large enough such that malicious users can't cycle through all keys from reasonably sized JWKS,
    // forcing cache eviction and thus introducing additional latency.
    cacheMaxSize: Long = 1000,
    cacheExpirationTime: Long = 10,
    cacheExpirationUnit: TimeUnit = TimeUnit.HOURS,
    connectionTimeout: Long = 10,
    connectionTimeoutUnit: TimeUnit = TimeUnit.SECONDS,
    readTimeout: Long = 10,
    readTimeoutUnit: TimeUnit = TimeUnit.SECONDS,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
) extends JwtVerifierBase {

  private[this] val http =
    new UrlJwkProvider(
      url,
      Integer.valueOf(connectionTimeoutUnit.toMillis(connectionTimeout).toInt),
      Integer.valueOf(readTimeoutUnit.toMillis(readTimeout).toInt),
    )

  private[this] val cache: Cache[String, JwtVerifier] = CacheBuilder
    .newBuilder()
    .maximumSize(cacheMaxSize)
    .expireAfterWrite(cacheExpirationTime, cacheExpirationUnit)
    .build()

  private[this] def getVerifier(keyId: String): Error \/ JwtVerifier =
    try {
      val jwk = http.get(keyId)
      val publicKey = jwk.getPublicKey
      publicKey match {
        case rsa: RSAPublicKey => RSA256Verifier(rsa, jwtTimestampLeeway)
        case ec: ECPublicKey if ec.getParams.getCurve.getField.getFieldSize == 256 =>
          ECDSAVerifier(Algorithm.ECDSA256(ec, null), jwtTimestampLeeway)
        case ec: ECPublicKey if ec.getParams.getCurve.getField.getFieldSize == 521 =>
          ECDSAVerifier(Algorithm.ECDSA512(ec, null), jwtTimestampLeeway)
        case key =>
          -\/(Error(Symbol("getVerifier"), s"Unsupported public key format ${key.getFormat}"))
      }
    } catch {
      case e: JwkException => -\/(Error(Symbol("getVerifier"), s"Couldn't get jwk from http: $e"))
      case _: Throwable =>
        -\/(Error(Symbol("getVerifier"), s"Unknown error while getting jwk from http"))
    }

  /** Looks up the verifier for the given keyId from the local cache.
    * On a cache miss, creates a new verifier by fetching the public key from the JWKS URL.
    */
  private[this] def getCachedVerifier(keyId: String): Error \/ JwtVerifier =
    if (keyId == null)
      -\/(Error(Symbol("getCachedVerifier"), "No Key ID found"))
    else
      \/.attempt(
        cache.get(keyId, () => getVerifier(keyId).fold(e => sys.error(e.shows), x => x))
      )(e => Error(Symbol("getCachedVerifier"), e.getMessage))

  def verify(jwt: Jwt): Error \/ DecodedJwt[String] =
    for {
      keyId <- \/.attempt(com.auth0.jwt.JWT.decode(jwt.value).getKeyId)(e =>
        Error(Symbol("verify"), e.getMessage)
      )
      verifier <- getCachedVerifier(keyId)
      decoded <- verifier.verify(jwt)
    } yield decoded
}

object JwksVerifier {
  def apply(url: String, jwtTimestampLeeway: Option[JwtTimestampLeeway] = None) =
    new JwksVerifier(new URI(url).toURL, jwtTimestampLeeway = jwtTimestampLeeway)

  final case class Error(what: Symbol, message: String)

  object Error {
    implicit val showInstance: Show[Error] =
      Show.shows(e => s"JwksVerifier.Error: ${e.what}, ${e.message}")
  }
}
