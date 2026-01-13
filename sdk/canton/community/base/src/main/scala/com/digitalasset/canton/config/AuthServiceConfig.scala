// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.auth0.jwt.algorithms.Algorithm
import com.daml.jwt.*
import com.digitalasset.canton.auth.{
  AccessLevel,
  AuthService,
  AuthServiceJWT,
  AuthServiceWildcard,
  AuthorizedUser,
  JwksVerifier,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.*
import com.digitalasset.canton.logging.NamedLoggerFactory

import scala.concurrent.duration.Duration

sealed trait AuthServiceConfig {

  def create(
      jwksCacheConfig: JwksCacheConfig,
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      loggerFactory: NamedLoggerFactory,
      maxTokenLife: NonNegativeDuration = NonNegativeDuration(Duration.Inf),
  ): AuthService
  def privileged: Boolean = false
  def users: Seq[AuthorizedUser] = Seq.empty
  def targetAudience: Option[String] = None
  def targetScope: Option[String] = None
  def maxTokenLife: NonNegativeDuration = NonNegativeDuration(Duration.Inf)
}

object AuthServiceConfig {
  import NonNegativeDurationConverter.*

  /** [default] Allows everything */
  case object Wildcard extends AuthServiceConfig {

    override def create(
        jwksCacheConfig: JwksCacheConfig,
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        loggerFactory: NamedLoggerFactory,
        maxTokenLife: NonNegativeDuration,
    ): AuthService =
      AuthServiceWildcard
  }

  /** [UNSAFE] Enables JWT-based authorization with shared secret HMAC256 signing: USE THIS
    * EXCLUSIVELY FOR TESTING
    */
  final case class UnsafeJwtHmac256(
      secret: NonEmptyString,
      override val targetAudience: Option[String],
      override val targetScope: Option[String],
      override val privileged: Boolean = false,
      accessLevel: AccessLevel = AccessLevel.Wildcard,
      override val users: Seq[AuthorizedUser] = Seq.empty,
      override val maxTokenLife: config.NonNegativeDuration = NonNegativeDuration(Duration.Inf),
  ) extends AuthServiceConfig {
    private def verifier(
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        maxTokenLife: Option[Long],
    ): JwtVerifier =
      HMAC256Verifier(secret.unwrap, jwtTimestampLeeway, maxTokenLife).fold(
        err =>
          throw new IllegalArgumentException(
            s"Failed to create HMAC256 verifier (secret: $secret): $err"
          ),
        identity,
      )

    override def create(
        jwksCacheConfig: JwksCacheConfig,
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        loggerFactory: NamedLoggerFactory,
        globalMaxTokenLife: NonNegativeDuration,
    ): AuthService =
      AuthServiceJWT(
        verifier(
          jwtTimestampLeeway,
          maxTokenLife.toMillisOrNone().orElse(globalMaxTokenLife.toMillisOrNone()),
        ),
        targetAudience,
        targetScope,
        privileged,
        accessLevel,
        loggerFactory,
        users,
      )

  }

  /** Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded
    * from the given X509 certificate file (.crt)
    */
  final case class JwtRs256Crt(
      certificate: String,
      override val targetAudience: Option[String],
      override val targetScope: Option[String],
      override val privileged: Boolean = false,
      accessLevel: AccessLevel = AccessLevel.Wildcard,
      override val users: Seq[AuthorizedUser] = Seq.empty,
      override val maxTokenLife: config.NonNegativeDuration = NonNegativeDuration(Duration.Inf),
  ) extends AuthServiceConfig {
    private def verifier(
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        maxTokenLife: Option[Long],
    ) = RSA256Verifier
      .fromCrtFile(certificate, jwtTimestampLeeway, maxTokenLife)
      .fold(
        err => throw new IllegalArgumentException(s"Failed to create RSA256 verifier: $err"),
        identity,
      )

    override def create(
        jwksCacheConfig: JwksCacheConfig,
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        loggerFactory: NamedLoggerFactory,
        globalMaxTokenLife: NonNegativeDuration,
    ): AuthService =
      AuthServiceJWT(
        verifier(
          jwtTimestampLeeway,
          maxTokenLife.toMillisOrNone().orElse(globalMaxTokenLife.toMillisOrNone()),
        ),
        targetAudience,
        targetScope,
        privileged,
        accessLevel,
        loggerFactory,
        users,
      )

  }

  /** "Enables JWT-based authorization, where the JWT is signed by ECDSA256 with a public key loaded
    * from the given X509 certificate file (.crt)"
    */
  final case class JwtEs256Crt(
      certificate: String,
      override val targetAudience: Option[String],
      override val targetScope: Option[String],
      override val privileged: Boolean = false,
      accessLevel: AccessLevel = AccessLevel.Wildcard,
      override val users: Seq[AuthorizedUser] = Seq.empty,
  ) extends AuthServiceConfig {
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    private def verifier(
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        maxTokenLife: Option[Long],
    ) = ECDSAVerifier
      .fromCrtFile(certificate, Algorithm.ECDSA256(_, null), jwtTimestampLeeway, maxTokenLife)
      .fold(
        err => throw new IllegalArgumentException(s"Failed to create ECDSA256 verifier: $err"),
        identity,
      )

    override def create(
        jwksCacheConfig: JwksCacheConfig,
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        loggerFactory: NamedLoggerFactory,
        globalMaxTokenLife: NonNegativeDuration,
    ): AuthService =
      AuthServiceJWT(
        verifier(
          jwtTimestampLeeway,
          maxTokenLife.toMillisOrNone().orElse(globalMaxTokenLife.toMillisOrNone()),
        ),
        targetAudience,
        targetScope,
        privileged,
        accessLevel,
        loggerFactory,
        users,
      )

  }

  /** Enables JWT-based authorization, where the JWT is signed by ECDSA512 with a public key loaded
    * from the given X509 certificate file (.crt)
    */
  final case class JwtEs512Crt(
      certificate: String,
      override val targetAudience: Option[String],
      override val targetScope: Option[String],
      override val privileged: Boolean = false,
      accessLevel: AccessLevel = AccessLevel.Wildcard,
      override val users: Seq[AuthorizedUser] = Seq.empty,
  ) extends AuthServiceConfig {
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    private def verifier(
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        maxTokenLife: Option[Long],
    ) = ECDSAVerifier
      .fromCrtFile(certificate, Algorithm.ECDSA512(_, null), jwtTimestampLeeway, maxTokenLife)
      .fold(
        err => throw new IllegalArgumentException(s"Failed to create ECDSA512 verifier: $err"),
        identity,
      )

    override def create(
        jwksCacheConfig: JwksCacheConfig,
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        loggerFactory: NamedLoggerFactory,
        globalMaxTokenLife: NonNegativeDuration,
    ): AuthService =
      AuthServiceJWT(
        verifier(
          jwtTimestampLeeway,
          maxTokenLife.toMillisOrNone().orElse(globalMaxTokenLife.toMillisOrNone()),
        ),
        targetAudience,
        targetScope,
        privileged,
        accessLevel,
        loggerFactory,
        users,
      )

  }

  /** Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded
    * from the given JWKS URL
    */
  final case class JwtJwks(
      url: NonEmptyString,
      override val targetAudience: Option[String],
      override val targetScope: Option[String],
      override val privileged: Boolean = false,
      accessLevel: AccessLevel = AccessLevel.Wildcard,
      override val users: Seq[AuthorizedUser] = Seq.empty,
  ) extends AuthServiceConfig {
    private def verifier(
        jwksCacheConfig: JwksCacheConfig,
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        maxTokenLife: Option[Long],
    ) =
      JwksVerifier(
        url.unwrap,
        jwksCacheConfig.cacheMaxSize,
        jwksCacheConfig.cacheExpiration.underlying,
        jwksCacheConfig.connectionTimeout.underlying,
        jwksCacheConfig.readTimeout.underlying,
        jwtTimestampLeeway,
        maxTokenLife,
      )

    override def create(
        jwksCacheConfig: JwksCacheConfig,
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        loggerFactory: NamedLoggerFactory,
        globalMaxTokenLife: NonNegativeDuration,
    ): AuthService =
      AuthServiceJWT(
        verifier(
          jwksCacheConfig,
          jwtTimestampLeeway,
          maxTokenLife.toMillisOrNone().orElse(globalMaxTokenLife.toMillisOrNone()),
        ),
        targetAudience,
        targetScope,
        privileged,
        accessLevel,
        loggerFactory,
        users,
      )
  }

}

object NonNegativeDurationConverter {
  implicit class NonNegativeDurationToMillisConverter(duration: NonNegativeDuration) {
    def toMillisOrNone(): Option[Long] =
      duration.duration match {
        case Duration.Inf => None
        case finite => Some(finite.toMillis)
      }
  }
}
