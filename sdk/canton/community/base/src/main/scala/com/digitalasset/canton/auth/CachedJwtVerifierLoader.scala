// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.auth0.jwk.{JwkException, UrlJwkProvider}
import com.auth0.jwt.algorithms.Algorithm
import com.daml.jwt.{
  ECDSAVerifier,
  Error as JwtError,
  JwksUrl,
  JwtException,
  JwtTimestampLeeway,
  JwtVerifier,
  RSA256Verifier,
}
import com.digitalasset.canton.auth.CachedJwtVerifierLoader.CacheKey
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.CacheMetrics
import com.github.blemale.scaffeine.Scaffeine

import java.security.interfaces.{ECPublicKey, RSAPublicKey}
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/** A JWK verifier loader, where the public keys are automatically fetched from the given JWKS URL.
  * The keys are then transformed into JWK Verifier
  *
  * The verifiers are kept in cache, in order to prevent having to do a remote network access for
  * each token validation.
  *
  * The cache is limited both in size and time. A size limit protects against infinitely growing
  * memory consumption. A time limit is a safety catch for the case where a public key is used to
  * sign a token without an expiration time and then is revoked.
  *
  * @param cacheMaxSize
  *   Maximum number of public keys to keep in the cache.
  * @param cacheExpiration
  *   Maximum time to keep public keys in the cache.
  * @param connectionTimeout
  *   Timeout for connecting to the JWKS URL.
  * @param readTimeout
  *   Timeout for reading from the JWKS URL.
  */
class CachedJwtVerifierLoader(
    // Large enough such that malicious users can't cycle through all keys from reasonably sized JWKS,
    // forcing cache eviction and thus introducing additional latency.
    cacheMaxSize: Long,
    cacheExpiration: FiniteDuration,
    connectionTimeout: FiniteDuration,
    readTimeout: FiniteDuration,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    maxTokenLife: Option[Long] = None,
    metrics: Option[CacheMetrics] = None,
    override protected val loggerFactory: NamedLoggerFactory,
) extends JwtVerifierLoader
    with NamedLogging
    with AutoCloseable {

  private val cache: ScaffeineCache.TunnelledAsyncLoadingCache[Future, CacheKey, JwtVerifier] =
    ScaffeineCache.buildAsync[Future, CacheKey, JwtVerifier](
      Scaffeine()
        .expireAfterWrite(cacheExpiration)
        .maximumSize(cacheMaxSize),
      loader = getVerifier,
      metrics = metrics,
    )(logger, "cache")

  override def loadJwtVerifier(jwksUrl: JwksUrl, keyId: Option[String]): Future[JwtVerifier] =
    cache.get(CacheKey(jwksUrl, keyId))

  private def jwkProvider(jwksUrl: JwksUrl) =
    new UrlJwkProvider(
      jwksUrl.toURL,
      connectionTimeout.toMillis.toInt,
      readTimeout.toMillis.toInt,
    )

  private def getVerifier(
      key: CacheKey
  ): Future[JwtVerifier] =
    fromDisjunction(getVerifierImpl(key))

  @SuppressWarnings(
    Array("org.wartremover.warts.Null")
  )
  private[this] def getVerifierImpl(cacheKey: CacheKey): Either[JwtError, JwtVerifier] =
    try {
      val jwk = jwkProvider(cacheKey.jwksUrl).get(cacheKey.keyId.orNull)
      val publicKey = jwk.getPublicKey
      publicKey match {
        case rsa: RSAPublicKey => RSA256Verifier(rsa, jwtTimestampLeeway, maxTokenLife)
        case ec: ECPublicKey if ec.getParams.getCurve.getField.getFieldSize == 256 =>
          ECDSAVerifier(Algorithm.ECDSA256(ec, null), jwtTimestampLeeway, maxTokenLife)
        case ec: ECPublicKey if ec.getParams.getCurve.getField.getFieldSize == 521 =>
          ECDSAVerifier(Algorithm.ECDSA512(ec, null), jwtTimestampLeeway, maxTokenLife)
        case key =>
          Left(JwtError(Symbol("getVerifier"), s"Unsupported public key format ${key.getFormat}"))
      }
    } catch {
      case e: JwkException => Left(JwtError(Symbol("getVerifier"), e.toString))
      case _: Throwable =>
        Left(JwtError(Symbol("getVerifier"), s"Unknown error while getting jwk from http"))
    }

  private def fromDisjunction[T](e: Either[JwtError, T]): Future[T] =
    e.fold(err => Future.failed(JwtException(err)), Future.successful)

  override def close(): Unit = {
    cache.invalidateAll()
    cache.cleanUp()
  }
}

object CachedJwtVerifierLoader {

  final case class CacheKey(
      jwksUrl: JwksUrl,
      keyId: Option[String],
  )
}
