// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import com.auth0.jwk.UrlJwkProvider
import com.daml.caching.CaffeineCache
import com.daml.caching.CaffeineCache.FutureAsyncCacheLoader
import com.daml.jwt.{JwtTimestampLeeway, JwtVerifier, RSA256Verifier}
import com.daml.ledger.api.domain.JwksUrl
import com.daml.metrics.Metrics
import com.github.benmanes.caffeine.{cache => caffeine}
import scalaz.\/
import com.daml.jwt.{Error => JwtError}
import com.daml.ledger.api.auth.CachedJwtVerifierLoader.CacheKey

import java.security.interfaces.RSAPublicKey
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}

/** A JWK verifier loader, where the public keys are automatically fetched from the given JWKS URL.
  * The keys are then transformed into JWK Verifier
  *
  * The verifiers are kept in cache, in order to prevent having to do a remote network access for each token validation.
  *
  * The cache is limited both in size and time.
  * A size limit protects against infinitely growing memory consumption.
  * A time limit is a safety catch for the case where a public key is used to sign a token without an expiration time
  * and then is revoked.
  *
  * @param cacheMaxSize Maximum number of public keys to keep in the cache.
  * @param cacheExpirationTime Maximum time to keep public keys in the cache.
  * @param connectionTimeout Timeout for connecting to the JWKS URL.
  * @param readTimeout Timeout for reading from the JWKS URL.
  */
class CachedJwtVerifierLoader(
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
    metrics: Metrics,
)(implicit
    executionContext: ExecutionContext
) extends JwtVerifierLoader {

  private val cache: CaffeineCache.AsyncLoadingCaffeineCache[
    CacheKey,
    JwtVerifier,
  ] =
    new CaffeineCache.AsyncLoadingCaffeineCache(
      caffeine.Caffeine
        .newBuilder()
        .expireAfterWrite(cacheExpirationTime, cacheExpirationUnit)
        .maximumSize(cacheMaxSize)
        .buildAsync(
          new FutureAsyncCacheLoader[CacheKey, JwtVerifier](key => getVerifier(key))
        ),
      metrics.daml.identityProviderConfigStore.verifierCache,
    )

  override def loadJwtVerifier(jwksUrl: JwksUrl, keyId: Option[String]): Future[JwtVerifier] =
    cache.get(CacheKey(jwksUrl, keyId))

  private def jwkProvider(jwksUrl: JwksUrl) =
    new UrlJwkProvider(
      jwksUrl.toURL,
      Integer.valueOf(
        connectionTimeoutUnit.toMillis(connectionTimeout).toInt
      ),
      Integer.valueOf(readTimeoutUnit.toMillis(readTimeout).toInt),
    )

  private def getVerifier(
      key: CacheKey
  ): Future[JwtVerifier] =
    for {
      jwk <- Future(jwkProvider(key.jwksUrl).get(key.keyId.orNull))
      publicKey = jwk.getPublicKey.asInstanceOf[RSAPublicKey]
      verifier <- fromDisjunction(RSA256Verifier(publicKey, jwtTimestampLeeway))
    } yield verifier

  private def fromDisjunction[T](e: \/[JwtError, T]): Future[T] =
    e.fold(err => Future.failed(new Exception(err.message)), Future.successful)

}

object CachedJwtVerifierLoader {

  case class CacheKey(
      jwksUrl: JwksUrl,
      keyId: Option[String],
  )
}
