// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.auth0.jwt.algorithms.Algorithm
import com.daml.jwt.{
  ECDSAVerifier,
  HMAC256Verifier,
  JwksVerifier,
  JwtTimestampLeeway,
  JwtVerifier,
  RSA256Verifier,
}
import com.digitalasset.canton.config.CantonRequireTypes.*
import com.digitalasset.canton.ledger.api.auth.{AuthService, AuthServiceJWT, AuthServiceWildcard}
import com.digitalasset.canton.logging.NamedLoggerFactory

sealed trait AuthServiceConfig {
  def create(
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      loggerFactory: NamedLoggerFactory,
  ): AuthService
}

object AuthServiceConfig {

  /** [default] Allows everything */
  object Wildcard extends AuthServiceConfig {
    override def create(
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        loggerFactory: NamedLoggerFactory,
    ): AuthService =
      AuthServiceWildcard
  }

  /** [UNSAFE] Enables JWT-based authorization with shared secret HMAC256 signing: USE THIS EXCLUSIVELY FOR TESTING */
  final case class UnsafeJwtHmac256(
      secret: NonEmptyString,
      targetAudience: Option[String],
      targetScope: Option[String],
  ) extends AuthServiceConfig {
    private def verifier(jwtTimestampLeeway: Option[JwtTimestampLeeway]): JwtVerifier =
      HMAC256Verifier(secret.unwrap, jwtTimestampLeeway).valueOr(err =>
        throw new IllegalArgumentException(
          s"Failed to create HMAC256 verifier (secret: $secret): $err"
        )
      )
    override def create(
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        loggerFactory: NamedLoggerFactory,
    ): AuthService =
      AuthServiceJWT(verifier(jwtTimestampLeeway), targetAudience, targetScope, loggerFactory)

  }

  /** Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given X509 certificate file (.crt) */
  final case class JwtRs256Crt(
      certificate: String,
      targetAudience: Option[String],
      targetScope: Option[String],
  ) extends AuthServiceConfig {
    private def verifier(jwtTimestampLeeway: Option[JwtTimestampLeeway]) = RSA256Verifier
      .fromCrtFile(certificate, jwtTimestampLeeway)
      .valueOr(err => throw new IllegalArgumentException(s"Failed to create RSA256 verifier: $err"))
    override def create(
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        loggerFactory: NamedLoggerFactory,
    ): AuthService =
      AuthServiceJWT(verifier(jwtTimestampLeeway), targetAudience, targetScope, loggerFactory)

  }

  /** "Enables JWT-based authorization, where the JWT is signed by ECDSA256 with a public key loaded from the given X509 certificate file (.crt)" */
  final case class JwtEs256Crt(
      certificate: String,
      targetAudience: Option[String],
      targetScope: Option[String],
  ) extends AuthServiceConfig {
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    private def verifier(jwtTimestampLeeway: Option[JwtTimestampLeeway]) = ECDSAVerifier
      .fromCrtFile(certificate, Algorithm.ECDSA256(_, null), jwtTimestampLeeway)
      .valueOr(err =>
        throw new IllegalArgumentException(s"Failed to create ECDSA256 verifier: $err")
      )
    override def create(
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        loggerFactory: NamedLoggerFactory,
    ): AuthService =
      AuthServiceJWT(verifier(jwtTimestampLeeway), targetAudience, targetScope, loggerFactory)

  }

  /** Enables JWT-based authorization, where the JWT is signed by ECDSA512 with a public key loaded from the given X509 certificate file (.crt) */
  final case class JwtEs512Crt(
      certificate: String,
      targetAudience: Option[String],
      targetScope: Option[String],
  ) extends AuthServiceConfig {
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    private def verifier(jwtTimestampLeeway: Option[JwtTimestampLeeway]) = ECDSAVerifier
      .fromCrtFile(certificate, Algorithm.ECDSA512(_, null), jwtTimestampLeeway)
      .valueOr(err =>
        throw new IllegalArgumentException(s"Failed to create ECDSA512 verifier: $err")
      )
    override def create(
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        loggerFactory: NamedLoggerFactory,
    ): AuthService =
      AuthServiceJWT(verifier(jwtTimestampLeeway), targetAudience, targetScope, loggerFactory)

  }

  /** Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given JWKS URL */
  final case class JwtRs256Jwks(
      url: NonEmptyString,
      targetAudience: Option[String],
      targetScope: Option[String],
  ) extends AuthServiceConfig {
    private def verifier(jwtTimestampLeeway: Option[JwtTimestampLeeway]) =
      JwksVerifier(url.unwrap, jwtTimestampLeeway)
    override def create(
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        loggerFactory: NamedLoggerFactory,
    ): AuthService =
      AuthServiceJWT(verifier(jwtTimestampLeeway), targetAudience, targetScope, loggerFactory)

  }

}
