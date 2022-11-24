// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import com.auth0.jwk.UrlJwkProvider
import com.auth0.jwt.JWT
import com.auth0.jwt.interfaces.DecodedJWT
import com.daml.jwt.{JwtTimestampLeeway, JwtVerifier, RSA256Verifier}
import com.daml.ledger.api.domain.{IdentityProviderId, JwksUrl}
import com.google.common.cache.{Cache, CacheBuilder}
import org.slf4j.{Logger, LoggerFactory}
import scalaz.syntax.show._
import scalaz.{-\/, \/, \/-}
import spray.json._

import java.security.interfaces.RSAPublicKey
import java.util.concurrent.{CompletableFuture, CompletionStage, TimeUnit}

class IdentityProviderAuthService(
    config: IdentityProviderAuthService.Config
) {

  private val logger: Logger = LoggerFactory.getLogger(IdentityProviderAuthService.getClass)

  private val cache: Cache[String, JwtVerifier] = CacheBuilder
    .newBuilder()
    .maximumSize(config.cache.maxSize)
    .expireAfterWrite(config.cache.expirationTime, config.cache.expirationUnit)
    .build()

  def decodeMetadata(
      authorizationHeader: Option[String],
      entries: Seq[IdentityProviderAuthService.Entry],
  ): CompletionStage[ClaimSet] =
    CompletableFuture.completedFuture {
      authorizationHeader match {
        case None => ClaimSet.Unauthenticated
        case Some(header) =>
          parseJWTPayload(header, entries).fold(
            error => {
              logger.warn("Authorization error: " + error.show)
              ClaimSet.Unauthenticated
            },
            identity,
          )
      }
    }

  private def parseJWTPayload(
      header: String,
      entries: Seq[IdentityProviderAuthService.Entry],
  ): JwtVerifier.Error \/ ClaimSet =
    for {
      token <- \/.fromEither(JwtVerifier.fromHeader(header))
      decodedJWT <- decode(token)
      claims <- extractClaims(
        token,
        Option(decodedJWT.getIssuer),
        Option(decodedJWT.getKeyId),
        entries,
      )
    } yield claims

  private def decode(token: String): JwtVerifier.Error \/ DecodedJWT =
    \/.attempt(JWT.decode(token))(e => JwtVerifier.Error(Symbol("JWT.decode"), e.getMessage))

  def extractClaims(
      token: String,
      issuer: Option[String],
      keyId: Option[String],
      entries: Seq[IdentityProviderAuthService.Entry],
  ): JwtVerifier.Error \/ ClaimSet = {
    issuer match {
      case None => \/-(ClaimSet.Unauthenticated)
      case Some(issuer) =>
        for {
          entry <- entryByIssuer(issuer, entries)
          verifier <- getCachedVerifier(entry, keyId)
          decoded <- verifier.verify(com.daml.jwt.domain.Jwt(token))
          parsed <- parsePayload(decoded.payload)
        } yield toAuthenticatedUser(parsed, entry.id)
    }
  }

  private def entryByIssuer(
      issuer: String,
      entries: Seq[IdentityProviderAuthService.Entry],
  ): JwtVerifier.Error \/ IdentityProviderAuthService.Entry =
    \/.fromEither(
      entries
        .find(e => issuer == e.issuer)
        .toRight(
          JwtVerifier.Error(Symbol("entryByIssuer"), s"Could not find an entry by issuer=$issuer")
        )
    )

  private def getCachedVerifier(
      entry: IdentityProviderAuthService.Entry,
      keyId: Option[String],
  ): JwtVerifier.Error \/ JwtVerifier =
    keyId match {
      case None =>
        -\/(JwtVerifier.Error(Symbol("getCachedVerifier"), "No Key ID found"))
      case Some(keyId) =>
        \/.attempt(
          cache.get(keyId, () => getVerifier(entry, keyId).fold(e => sys.error(e.shows), identity))
        )(e => JwtVerifier.Error(Symbol("getCachedVerifier"), e.getMessage))
    }

  private def getVerifier(
      entry: IdentityProviderAuthService.Entry,
      keyId: String,
  ): JwtVerifier.Error \/ JwtVerifier = {
    val jwk = jwkProvider(entry.jwksURL).get(keyId)
    val publicKey = jwk.getPublicKey.asInstanceOf[RSAPublicKey]
    RSA256Verifier(publicKey, config.jwtTimestampLeeway)
  }

  private def jwkProvider(jwksUrl: JwksUrl) =
    new UrlJwkProvider(
      jwksUrl.toURL,
      Integer.valueOf(
        config.http.connectionTimeoutUnit.toMillis(config.http.connectionTimeout).toInt
      ),
      Integer.valueOf(config.http.readTimeoutUnit.toMillis(config.http.readTimeout).toInt),
    )

  private def parsePayload(
      jwtPayload: String
  ): JwtVerifier.Error \/ StandardJWTPayload = {

    def toError(t: Throwable) =
      JwtVerifier.Error(Symbol("parsePayload"), "Could not parse JWT token: " + t.getMessage)

    \/.attempt(parse(jwtPayload))(toError)
      .flatMap {
        case _: CustomDamlJWTPayload =>
          -\/(JwtVerifier.Error(Symbol("parsePayload"), "Unexpected token format"))
        case payload: StandardJWTPayload =>
          \/-(payload)
      }
  }

  private def parse(jwtPayload: String): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.JsonImplicits._
    JsonParser(jwtPayload).convertTo[AuthServiceJWTPayload]
  }

  private def toAuthenticatedUser(payload: StandardJWTPayload, id: IdentityProviderId.Id) =
    ClaimSet.AuthenticatedUser(
      identityProviderId = id,
      participantId = payload.participantId,
      userId = payload.userId,
      expiration = payload.exp,
    )
}

object IdentityProviderAuthService {
  case class Entry(
      id: IdentityProviderId.Id,
      jwksURL: JwksUrl,
      issuer: String,
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
