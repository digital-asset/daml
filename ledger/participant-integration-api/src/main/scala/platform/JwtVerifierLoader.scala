// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.auth0.jwk.UrlJwkProvider
import com.daml.caching.CaffeineCache
import com.daml.caching.CaffeineCache.FutureAsyncCacheLoader
import com.daml.jwt.{JwtTimestampLeeway, JwtVerifier, RSA256Verifier}
import com.daml.ledger.api.domain.JwksUrl
import com.daml.metrics.Metrics
import com.daml.platform.CachedJwtVerifierLoader.CacheKey
import com.github.benmanes.caffeine.{cache => caffeine}
import scalaz.\/

import java.security.interfaces.RSAPublicKey
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}

trait JwtVerifierLoader {
  def loadJwtVerifier(jwksUrl: JwksUrl, keyId: Option[String]): Future[JwtVerifier]
}

class CachedJwtVerifierLoader(
    config: CachedJwtVerifierLoader.Config,
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
        .expireAfterWrite(config.cache.expirationTime, config.cache.expirationUnit)
        .maximumSize(config.cache.maxSize)
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
        config.http.connectionTimeoutUnit.toMillis(config.http.connectionTimeout).toInt
      ),
      Integer.valueOf(config.http.readTimeoutUnit.toMillis(config.http.readTimeout).toInt),
    )

  private def getVerifier(
      key: CacheKey
  ): Future[JwtVerifier] =
    for {
      jwk <- Future(jwkProvider(key.jwksUrl).get(key.keyId.orNull))
      publicKey = jwk.getPublicKey.asInstanceOf[RSAPublicKey]
      verifier <- fromDisjunction(RSA256Verifier(publicKey, config.jwtTimestampLeeway))
    } yield verifier

  private def fromDisjunction[T](e: \/[JwtVerifier.Error, T]): Future[T] =
    e.fold(err => Future.failed(new Exception(err.message)), Future.successful)

}

object CachedJwtVerifierLoader {

  case class CacheKey(
      jwksUrl: JwksUrl,
      keyId: Option[String],
  )

  case class CacheConfig(
      // Large enough such that malicious users can't cycle through all keys from reasonably sized JWKS,
      // forcing cache eviction and thus introducing additional latency.
      maxSize: Long = 1000,
      expirationTime: Long = 10,
      expirationUnit: TimeUnit = TimeUnit.HOURS,
  )

  case class HttpConfig(
      connectionTimeout: Long = 10,
      connectionTimeoutUnit: TimeUnit = TimeUnit.SECONDS,
      readTimeout: Long = 10,
      readTimeoutUnit: TimeUnit = TimeUnit.SECONDS,
  )

  case class Config(
      cache: CacheConfig = CacheConfig(),
      http: HttpConfig = HttpConfig(),
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
  )
}
