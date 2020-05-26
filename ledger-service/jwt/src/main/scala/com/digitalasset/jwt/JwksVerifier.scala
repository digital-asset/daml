// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import java.net.{URI, URL}
import java.security.interfaces.RSAPublicKey
import java.util.concurrent.TimeUnit

import com.auth0.jwk.UrlJwkProvider
import com.daml.jwt.JwtVerifier.Error
import com.google.common.cache.{Cache, CacheBuilder}
import scalaz.{-\/, Show, \/}
import scalaz.syntax.show._

/**
  * A JWK verifier, where the public keys are automatically fetched from the given JWKS URL.
  *
  * In JWKS, each key ID uniquely identifies a public key.
  * The keys are kept in cache, in order to prevent having to do a remove network access for each token validation.
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
    cacheMaxSize: Long = 10,
    cacheExpirationTime: Long = 10,
    cacheExpirationUnit: TimeUnit = TimeUnit.HOURS,
    connectionTimeout: Long = 10,
    connectionTimeoutUnit: TimeUnit = TimeUnit.SECONDS,
    readTimeout: Long = 10,
    readTimeoutUnit: TimeUnit = TimeUnit.SECONDS,
) extends JwtVerifierBase {

  private[this] val http =
    new UrlJwkProvider(
      url,
      new Integer(connectionTimeoutUnit.toMillis(connectionTimeout).toInt),
      new Integer(readTimeoutUnit.toMillis(readTimeout).toInt),
    )

  private[this] val cache: Cache[String, JwtVerifier] = CacheBuilder
    .newBuilder()
    .maximumSize(cacheMaxSize)
    .expireAfterWrite(cacheExpirationTime, cacheExpirationUnit)
    .build()

  private[this] def getVerifier(keyId: String): Error \/ JwtVerifier = {
    val jwk = http.get(keyId)
    val publicKey = jwk.getPublicKey.asInstanceOf[RSAPublicKey]
    RSA256Verifier(publicKey)
  }

  /** Looks up the verifier for the given keyId from the local cache.
    * On a cache miss, creates a new verifier by fetching the public key from the JWKS URL. */
  private[this] def getCachedVerifier(keyId: String): Error \/ JwtVerifier = {
    if (keyId == null)
      -\/(Error('getCachedVerifier, "No Key ID found"))
    else
      \/.fromTryCatchNonFatal(
        cache.get(keyId, () => getVerifier(keyId).fold(e => sys.error(e.shows), x => x))
      ).leftMap(e => Error('getCachedVerifier, e.getMessage))
  }

  def verify(jwt: domain.Jwt): Error \/ domain.DecodedJwt[String] = {
    for {
      keyId <- \/.fromTryCatchNonFatal(com.auth0.jwt.JWT.decode(jwt.value).getKeyId)
        .leftMap(e => Error('verify, e.getMessage))
      verifier <- getCachedVerifier(keyId)
      decoded <- verifier.verify(jwt)
    } yield decoded
  }
}

object JwksVerifier {
  def apply(url: String) = new JwksVerifier(new URI(url).toURL)

  final case class Error(what: Symbol, message: String)

  object Error {
    implicit val showInstance: Show[Error] =
      Show.shows(e => s"JwksVerifier.Error: ${e.what}, ${e.message}")
  }
}
