// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.{Verification}

final case class JwtTimestampLeeway(
    default: Option[Long] = None,
    expiresAt: Option[Long] = None,
    issuedAt: Option[Long] = None,
    notBefore: Option[Long] = None,
)

trait Leeway {
  def getVerifier(
      algorithm: Algorithm,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
  ): com.auth0.jwt.interfaces.JWTVerifier = {
    def addLeeway(
        verification: Verification,
        jwtTimestampLeeway: JwtTimestampLeeway,
    ): Verification = {
      val mbOptionsActions: List[(Option[Long], (Verification, Long) => Verification)] = List(
        (jwtTimestampLeeway.default, _.acceptLeeway(_)),
        (jwtTimestampLeeway.expiresAt, _.acceptExpiresAt(_)),
        (jwtTimestampLeeway.issuedAt, _.acceptIssuedAt(_)),
        (jwtTimestampLeeway.notBefore, _.acceptNotBefore(_)),
      )
      mbOptionsActions.foldLeft(verification) {
        case (verifier, (None, _)) => verifier
        case (verifier, (Some(value), f)) => f(verifier, value)
      }
    }
    val defaultVerifier = JWT.require(algorithm)
    val verification = jwtTimestampLeeway.fold(defaultVerifier)(addLeeway(defaultVerifier, _))
    verification.build()
  }
}
