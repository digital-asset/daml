// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import com.auth0.jwk.UrlJwkProvider
import com.daml.jwt.{JwtTimestampLeeway, JwtVerifier, RSA256Verifier}
import com.google.common.cache.{Cache, CacheBuilder}
import scalaz.{-\/, \/, \/-}
import scalaz.syntax.show._

import java.net.URL
import java.security.interfaces.RSAPublicKey
import java.util.concurrent.TimeUnit
import java.util.concurrent.{CompletableFuture, CompletionStage}
import io.grpc.Metadata
import org.slf4j.{Logger, LoggerFactory}
import spray.json._

class IdentityProviderAuthService(
    url: URL,
    expectedIssuer: String,
    config: IdentityProviderAuthService.Config,
) extends AuthService {

  private val logger: Logger = LoggerFactory.getLogger(IdentityProviderAuthService.getClass)

  private val http =
    new UrlJwkProvider(
      url,
      Integer.valueOf(
        config.http.connectionTimeoutUnit.toMillis(config.http.connectionTimeout).toInt
      ),
      Integer.valueOf(config.http.readTimeoutUnit.toMillis(config.http.readTimeout).toInt),
    )

  private val cache: Cache[String, JwtVerifier] = CacheBuilder
    .newBuilder()
    .maximumSize(config.cache.maxSize)
    .expireAfterWrite(config.cache.expirationTime, config.cache.expirationUnit)
    .build()

  private def getVerifier(keyId: String): JwtVerifier.Error \/ JwtVerifier = {
    val jwk = http.get(keyId)
    val publicKey = jwk.getPublicKey.asInstanceOf[RSAPublicKey]
    RSA256Verifier(publicKey, config.jwtTimestampLeeway)
  }

  /** Looks up the verifier for the given keyId from the local cache.
    * On a cache miss, creates a new verifier by fetching the public key from the JWKS URL.
    */
  private def getCachedVerifier(keyId: String): JwtVerifier.Error \/ JwtVerifier =
    if (keyId == null)
      -\/(JwtVerifier.Error(Symbol("getCachedVerifier"), "No Key ID found"))
    else
      \/.attempt(
        cache.get(keyId, () => getVerifier(keyId).fold(e => sys.error(e.shows), x => x))
      )(e => JwtVerifier.Error(Symbol("getCachedVerifier"), e.getMessage))

  private def verify(
      jwt: com.daml.jwt.domain.Jwt
  ): JwtVerifier.Error \/ com.daml.jwt.domain.DecodedJwt[String] =
    for {
      keyId <- \/.attempt(com.auth0.jwt.JWT.decode(jwt.value).getKeyId)(e =>
        JwtVerifier.Error(Symbol("verify"), e.getMessage)
      )
      verifier <- getCachedVerifier(keyId)
      decoded <- verifier.verify(jwt)
    } yield decoded

  override def decodeMetadata(headers: Metadata): CompletionStage[ClaimSet] =
    CompletableFuture.completedFuture {
      getAuthorizationHeader(headers) match {
        case None => ClaimSet.Unauthenticated
        case Some(header) => parseHeader(header)
      }
    }

  private def getAuthorizationHeader(headers: Metadata): Option[String] =
    Option(headers.get(AUTHORIZATION_KEY))

  private def parseHeader(header: String): ClaimSet =
    parseJWTPayload(header).fold(
      error => {
        logger.warn("Authorization error: " + error.message)
        ClaimSet.Unauthenticated
      },
      token => toAuthenticatedUser(token),
    )

  def parse(jwtPayload: String): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.JsonImplicits._
    JsonParser(jwtPayload).convertTo[AuthServiceJWTPayload]
  }

  private def parsePayload(
      jwtPayload: String
  ): JwtVerifier.Error \/ StandardJWTPayload = {

    def toError(t: Throwable) =
      JwtVerifier.Error(Symbol("parsePayload"), "Could not parse JWT token: " + t.getMessage)

    \/.attempt(parse(jwtPayload))(toError)
      .flatMap {
        case _: CustomDamlJWTPayload =>
          -\/(JwtVerifier.Error(Symbol("parsePayload"), "Unexpected token format"))
        case token: StandardJWTPayload if !token.issuer.contains(expectedIssuer) =>
          -\/(JwtVerifier.Error(Symbol("parsePayload"), "Unexpected token issuer"))
        case payload: StandardJWTPayload => \/-(payload)
      }
  }

  private def parseJWTPayload(
      header: String
  ): JwtVerifier.Error \/ StandardJWTPayload =
    for {
      token <- \/.fromEither(JwtVerifier.fromHeader(header))
      decoded <- verify(com.daml.jwt.domain.Jwt(token))
      parsed <- parsePayload(decoded.payload)
    } yield parsed

  private def toAuthenticatedUser(payload: StandardJWTPayload) = ClaimSet.AuthenticatedUser(
    issuer = payload.issuer,
    participantId = payload.participantId,
    userId = payload.userId,
    expiration = payload.exp,
  )
}

object IdentityProviderAuthService {
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
      cache: CacheConfig,
      http: HttpConfig,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
  )
}
