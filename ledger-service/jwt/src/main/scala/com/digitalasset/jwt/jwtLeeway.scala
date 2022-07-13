// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.{Verification}

final case class LeewayOptions(
    leeway: Option[Long] = None,
    expiresAt: Option[Long] = None,
    issuedAt: Option[Long] = None,
    notBefore: Option[Long] = None,
)

trait Leeway {
  def getVerifier(
      algorithm: Algorithm,
      mbLeewayOptions: Option[LeewayOptions] = None,
  ): com.auth0.jwt.interfaces.JWTVerifier = {
    def addLeeway(verification: Verification, leewayOptions: LeewayOptions): Verification = {
      val mbOptionsActions: List[(Option[Long], (Verification, Long) => Verification)] = List(
        (leewayOptions.leeway, _.acceptLeeway(_)),
        (leewayOptions.expiresAt, _.acceptExpiresAt(_)),
        (leewayOptions.issuedAt, _.acceptIssuedAt(_)),
        (leewayOptions.notBefore, _.acceptNotBefore(_)),
      )
      mbOptionsActions.foldLeft(verification) {
        case (verifier, (None, _)) => verifier
        case (verifier, (Some(value), f)) => f(verifier, value)
      }
    }
    val defaultVerifier = JWT.require(algorithm)
    val verification = mbLeewayOptions.fold(defaultVerifier)(addLeeway(defaultVerifier, _))
    verification.build()
  }
}
