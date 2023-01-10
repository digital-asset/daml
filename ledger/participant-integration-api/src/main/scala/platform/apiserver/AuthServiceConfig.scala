// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.auth0.jwt.algorithms.Algorithm
import com.daml.jwt.{
  ECDSAVerifier,
  HMAC256Verifier,
  JwksVerifier,
  JwtTimestampLeeway,
  JwtVerifier,
  RSA256Verifier,
}
import com.daml.ledger.api.auth.{AuthService, AuthServiceJWT, AuthServiceWildcard}

sealed trait AuthServiceConfig {
  def create(jwtTimestampLeeway: Option[JwtTimestampLeeway]): AuthService
}
object AuthServiceConfig {

  /** [default] Allows everything */
  final object Wildcard extends AuthServiceConfig {
    override def create(jwtTimestampLeeway: Option[JwtTimestampLeeway]): AuthService =
      AuthServiceWildcard
  }

  /** [UNSAFE] Enables JWT-based authorization with shared secret HMAC256 signing: USE THIS EXCLUSIVELY FOR TESTING */
  final case class UnsafeJwtHmac256(secret: String) extends AuthServiceConfig {
    private def verifier(jwtTimestampLeeway: Option[JwtTimestampLeeway]): JwtVerifier =
      HMAC256Verifier(secret, jwtTimestampLeeway).valueOr(err =>
        throw new IllegalArgumentException(
          s"Failed to create HMAC256 verifier (secret: $secret): $err"
        )
      )
    def create(jwtTimestampLeeway: Option[JwtTimestampLeeway]): AuthService = AuthServiceJWT(
      verifier(jwtTimestampLeeway)
    )
  }

  /** Enables JWT-based authorization, where the JWT is signed by RSA256 with the verifying public key loaded from the given X509 certificate file (.crt) */
  final case class JwtRs256(certificate: String) extends AuthServiceConfig {
    private def verifier(jwtTimestampLeeway: Option[JwtTimestampLeeway]) = RSA256Verifier
      .fromCrtFile(certificate, jwtTimestampLeeway)
      .valueOr(err => throw new IllegalArgumentException(s"Failed to create RSA256 verifier: $err"))
    override def create(jwtTimestampLeeway: Option[JwtTimestampLeeway]): AuthService =
      AuthServiceJWT(verifier(jwtTimestampLeeway))
  }

  /** "Enables JWT-based authorization, where the JWT is signed by ECDSA256 with the verifying public key loaded from the given X509 certificate file (.crt)" */
  final case class JwtEs256(certificate: String) extends AuthServiceConfig {
    private def verifier(jwtTimestampLeeway: Option[JwtTimestampLeeway]) = ECDSAVerifier
      .fromCrtFile(certificate, Algorithm.ECDSA256(_, null), jwtTimestampLeeway)
      .valueOr(err =>
        throw new IllegalArgumentException(s"Failed to create ECDSA256 verifier: $err")
      )
    override def create(jwtTimestampLeeway: Option[JwtTimestampLeeway]): AuthService =
      AuthServiceJWT(verifier(jwtTimestampLeeway))
  }

  /** Enables JWT-based authorization, where the JWT is signed by ECDSA512 with the verifying public key loaded from the given X509 certificate file (.crt) */
  final case class JwtEs512(certificate: String) extends AuthServiceConfig {
    private def verifier(jwtTimestampLeeway: Option[JwtTimestampLeeway]) = ECDSAVerifier
      .fromCrtFile(certificate, Algorithm.ECDSA512(_, null), jwtTimestampLeeway)
      .valueOr(err =>
        throw new IllegalArgumentException(s"Failed to create ECDSA512 verifier: $err")
      )
    override def create(jwtTimestampLeeway: Option[JwtTimestampLeeway]): AuthService =
      AuthServiceJWT(verifier(jwtTimestampLeeway))
  }

  /** Enables JWT-based authorization, where the JWT is signed by RSA256 with the verifying public key loaded from the given JWKS URL */
  final case class JwtRs256Jwks(url: String) extends AuthServiceConfig {
    private def verifier(jwtTimestampLeeway: Option[JwtTimestampLeeway]) =
      JwksVerifier(url, jwtTimestampLeeway)
    override def create(jwtTimestampLeeway: Option[JwtTimestampLeeway]): AuthService =
      AuthServiceJWT(verifier(jwtTimestampLeeway))
  }

}
