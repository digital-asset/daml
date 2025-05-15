// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.daml.jwt.{
  AuthServiceJWTCodec,
  CustomDamlJWTPayload,
  DecodedJwt,
  Jwt,
  JwtSigner,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import com.digitalasset.canton.platform.localstore.api.UserManagementStore

import java.time.Instant

/** some helpers in other to use with ad-hoc JWT authentication */
object JwtTokenUtilities {

  def buildUnsafeToken(
      secret: String,
      admin: Boolean,
      readAs: List[String],
      actAs: List[String],
      ledgerId: Option[String] = None,
      applicationId: Option[String] = None,
      exp: Option[Instant] = None,
  ): String = {
    val payload = CustomDamlJWTPayload(
      ledgerId = ledgerId,
      None,
      applicationId = applicationId,
      exp = exp,
      admin = admin,
      readAs = readAs,
      actAs = actAs,
    )
    // stolen from com.digitalasset.canton.ledger.api.auth.Main
    val jwtPayload = AuthServiceJWTCodec.compactPrint(payload)
    val jwtHeader = s"""{"alg": "HS256", "typ": "JWT"}"""
    val signed: Jwt = JwtSigner.HMAC256
      .sign(DecodedJwt(jwtHeader, jwtPayload), secret)
      .valueOr(err => throw new RuntimeException(err.message))
    signed.value
  }

  def buildUnsafeStandardToken(
      secret: String,
      userId: Option[String] = None,
      exp: Option[Instant] = None,
      scope: Option[String] = None,
  ): String = {
    val payload = StandardJWTPayload(
      issuer = None,
      userId = userId.getOrElse(UserManagementStore.DefaultParticipantAdminUserId),
      participantId = None,
      exp = exp,
      format = StandardJWTTokenFormat.Scope,
      audiences = List.empty,
      scope = scope.fold(Some(AuthServiceJWTCodec.scopeLedgerApiFull))(Some(_)),
    )
    // stolen from com.digitalasset.canton.ledger.api.auth.Main
    val jwtPayload = AuthServiceJWTCodec.compactPrint(payload)
    val jwtHeader = s"""{"alg": "HS256", "typ": "JWT"}"""
    val signed: Jwt = JwtSigner.HMAC256
      .sign(DecodedJwt(jwtHeader, jwtPayload), secret)
      .valueOr(err => throw new RuntimeException(err.message))
    signed.value
  }

}
