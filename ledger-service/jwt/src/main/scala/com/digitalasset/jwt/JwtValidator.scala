// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.jwt

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.digitalasset.jwt.JwtValidator.Error
import com.typesafe.scalalogging.StrictLogging
import scalaz.{Show, \/}

class JwtValidator(verifier: com.auth0.jwt.interfaces.JWTVerifier) {
  def validate(jwt: domain.Jwt): Error \/ domain.DecodedJwt[String] = {
    \/.fromTryCatchNonFatal(verifier.verify(jwt.value))
      .bimap(
        e => Error('validate, e.getMessage),
        a => domain.DecodedJwt(header = a.getHeader, payload = a.getPayload)
      )
  }
}

object JwtValidator {
  type ValidateJwt = domain.Jwt => Error \/ domain.DecodedJwt[String]

  final case class Error(what: Symbol, message: String)

  object Error {
    implicit val showInstance: Show[Error] =
      Show.shows(e => s"JwtValidator.Error: ${e.what}, ${e.message}")
  }
}

// HMAC256 validator factory
object HMAC256 extends StrictLogging {
  def apply(issuer: String, secret: String): Error \/ JwtValidator =
    \/.fromTryCatchNonFatal {
      logger.warn(
        "HMAC256 JWT Validator is NOT recommended for production env, please use RSA256 (WIP)!!!")

      val algorithm = Algorithm.HMAC256(secret)
      val verifier = JWT.require(algorithm).withIssuer(issuer).build()
      new JwtValidator(verifier)
    }.leftMap(e => Error('HMAC256, e.getMessage))
}

// TODO(Leo) RSA256 validator
