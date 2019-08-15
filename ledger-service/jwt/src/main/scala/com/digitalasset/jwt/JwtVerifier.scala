// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.jwt

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.digitalasset.jwt.JwtVerifier.Error
import com.typesafe.scalalogging.StrictLogging
import scalaz.{Show, \/}
import scalaz.syntax.show._
import scalaz.syntax.traverse._

class JwtVerifier(verifier: com.auth0.jwt.interfaces.JWTVerifier) {

  def verify(jwt: domain.Jwt): Error \/ domain.DecodedJwt[String] = {
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
        "HMAC256 JWT Validator is NOT recommended for production env, please use RSA256 (WIP)!!!")

      val algorithm = Algorithm.HMAC256(secret)
      val verifier = JWT.require(algorithm).build()
      new JwtVerifier(verifier)
    }.leftMap(e => Error('HMAC256, e.getMessage))
}

// TODO(Leo) RSA256 validator
