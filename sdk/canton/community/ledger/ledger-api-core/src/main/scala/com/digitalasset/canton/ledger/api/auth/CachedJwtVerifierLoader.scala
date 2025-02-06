// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.auth0.jwk.UrlJwkProvider
import com.daml.jwt.{Error as JwtError, JwtTimestampLeeway, JwtVerifier, RSA256Verifier}
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.ledger.api.JwksUrl
import com.digitalasset.canton.ledger.api.auth.CachedJwtVerifierLoader.CacheKey
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.github.blemale.scaffeine.Scaffeine
import scalaz.\/

import java.security.interfaces.RSAPublicKey
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DurationInt, FiniteDuration}
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
    cacheExpiration: FiniteDuration = 10.hours,
    connectionTimeout: Long = 10,
    connectionTimeoutUnit: TimeUnit = TimeUnit.SECONDS,
    readTimeout: Long = 10,
    readTimeoutUnit: TimeUnit = TimeUnit.SECONDS,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    metrics: LedgerApiServerMetrics,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends JwtVerifierLoader
    with NamedLogging {

  private val cache: ScaffeineCache.TunnelledAsyncLoadingCache[Future, CacheKey, JwtVerifier] =
    ScaffeineCache.buildAsync[Future, CacheKey, JwtVerifier](
      Scaffeine()
        .expireAfterWrite(cacheExpiration)
        .maximumSize(cacheMaxSize),
      loader = getVerifier,
      metrics = Some(metrics.identityProviderConfigStore.verifierCache),
    )(logger, "cache")

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

  @SuppressWarnings(
    Array("org.wartremover.warts.Null", "org.wartremover.warts.AsInstanceOf")
  ) // interfacing with java
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

  final case class CacheKey(
      jwksUrl: JwksUrl,
      keyId: Option[String],
  )
}
