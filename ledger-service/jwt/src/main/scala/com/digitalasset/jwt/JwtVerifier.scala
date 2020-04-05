// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import java.io.File
import java.security.interfaces.{ECPublicKey, RSAPublicKey}

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.{RSAKeyProvider}
import com.daml.jwt.JwtVerifier.Error
import com.typesafe.scalalogging.StrictLogging
import scalaz.{Show, \/}
import scalaz.syntax.show._
import scalaz.syntax.traverse._

abstract class JwtVerifierBase {
  def verify(jwt: domain.Jwt): Error \/ domain.DecodedJwt[String]
}

class JwtVerifier(verifier: com.auth0.jwt.interfaces.JWTVerifier) extends JwtVerifierBase {

  def verify(jwt: domain.Jwt): Error \/ domain.DecodedJwt[String] = {
    // The auth0 library verification already fails if the token has expired,
    // but we still need to do manual expiration checks in ongoing streams
    \/.fromTryCatchNonFatal(verifier.verify(jwt.value))
      .bimap(
        e => Error('verify, e.getMessage),
        a => domain.DecodedJwt(header = a.getHeader, payload = a.getPayload)
      )
      .flatMap(base64Decode)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def base64Decode(jwt: domain.DecodedJwt[String]): Error \/ domain.DecodedJwt[String] =
    jwt.traverse(Base64.decode).leftMap(e => Error('base64Decode, e.shows))

}

object JwtVerifier {
  final case class Error(what: Symbol, message: String)

  object Error {
    implicit val showInstance: Show[Error] =
      Show.shows(e => s"JwtVerifier.Error: ${e.what}, ${e.message}")
  }
}

// HMAC256 validator factory
object HMAC256Verifier extends StrictLogging {
  def apply(secret: String): Error \/ JwtVerifier =
    \/.fromTryCatchNonFatal {
      logger.warn(
        "HMAC256 JWT Validator is NOT recommended for production environments, please use RSA256!!!")

      val algorithm = Algorithm.HMAC256(secret)
      val verifier = JWT.require(algorithm).build()
      new JwtVerifier(verifier)
    }.leftMap(e => Error('HMAC256, e.getMessage))
}

// ECDSA validator factory
object ECDSAVerifier extends StrictLogging {
  def apply(algorithm: Algorithm): Error \/ JwtVerifier =
    \/.fromTryCatchNonFatal {
      val verifier = JWT.require(algorithm).build()
      new JwtVerifier(verifier)
    }.leftMap(e => Error(Symbol(algorithm.getName), e.getMessage))

  def fromCrtFile(
      path: String,
      algorithmPublicKey: ECPublicKey => Algorithm): Error \/ JwtVerifier = {
    for {
      key <- \/.fromEither(
        KeyUtils
          .readECPublicKeyFromCrt(new File(path))
          .toEither)
        .leftMap(e => Error('fromCrtFile, e.getMessage))
      verifier <- ECDSAVerifier(algorithmPublicKey(key))
    } yield verifier
  }
}

// RSA256 validator factory
object RSA256Verifier extends StrictLogging {
  def apply(publicKey: RSAPublicKey): Error \/ JwtVerifier =
    \/.fromTryCatchNonFatal {

      val algorithm = Algorithm.RSA256(publicKey, null)
      val verifier = JWT.require(algorithm).build()
      new JwtVerifier(verifier)
    }.leftMap(e => Error('RSA256, e.getMessage))

  def apply(keyProvider: RSAKeyProvider): Error \/ JwtVerifier =
    \/.fromTryCatchNonFatal {

      val algorithm = Algorithm.RSA256(keyProvider)
      val verifier = JWT.require(algorithm).build()
      new JwtVerifier(verifier)
    }.leftMap(e => Error('RSA256, e.getMessage))

  /**
    * Create a RSA256 validator with the key loaded from the given file.
    * The file is assumed to be a X509 encoded certificate.
    * These typically have the .crt file extension.
    */
  def fromCrtFile(path: String): Error \/ JwtVerifier = {
    for {
      rsaKey <- \/.fromEither(
        KeyUtils
          .readRSAPublicKeyFromCrt(new File(path))
          .toEither)
        .leftMap(e => Error('fromCrtFile, e.getMessage))
      verfier <- RSA256Verifier.apply(rsaKey)
    } yield verfier
  }
}
