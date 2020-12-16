// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import java.time.{Duration, Instant}
import java.util.UUID

import com.daml.jwt.domain.DecodedJwt
import com.daml.jwt.{HMAC256Verifier, JwtSigner}
import com.daml.ledger.api.auth.{
  AuthService,
  AuthServiceJWT,
  AuthServiceJWTCodec,
  AuthServiceJWTPayload
}
import com.daml.ledger.api.domain.LedgerId
import org.scalatest.Suite
import scalaz.syntax.tag.ToTagOps

trait SandboxRequiringAuthorization {
  self: Suite with AbstractSandboxFixture =>

  private val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
  private val jwtSecret = UUID.randomUUID.toString

  protected val emptyToken: AuthServiceJWTPayload = AuthServiceJWTPayload(
    ledgerId = None,
    participantId = None,
    applicationId = None,
    exp = None,
    admin = false,
    actAs = Nil,
    readAs = Nil,
  )

  protected val adminToken: AuthServiceJWTPayload = emptyToken.copy(admin = true)

  protected lazy val wrappedLedgerId: LedgerId = ledgerId(Some(toHeader(adminToken)))
  protected lazy val unwrappedLedgerId: String = wrappedLedgerId.unwrap

  override protected def authService: Option[AuthService] = {
    val jwtVerifier =
      HMAC256Verifier(jwtSecret).getOrElse(sys.error("Failed to create HMAC256 verifier"))
    Some(AuthServiceJWT(jwtVerifier))
  }

  protected def readOnlyToken(party: String): AuthServiceJWTPayload =
    emptyToken.copy(readAs = List(party))

  protected def readWriteToken(party: String): AuthServiceJWTPayload =
    emptyToken.copy(actAs = List(party))

  protected def multiPartyToken(actAs: List[String], readAs: List[String]): AuthServiceJWTPayload =
    emptyToken.copy(actAs = actAs, readAs = readAs)

  protected def expiringIn(t: Duration, p: AuthServiceJWTPayload): AuthServiceJWTPayload =
    p.copy(exp = Option(Instant.now().plusNanos(t.toNanos)))

  protected def forLedgerId(id: String, p: AuthServiceJWTPayload): AuthServiceJWTPayload =
    p.copy(ledgerId = Some(id))

  protected def forParticipantId(id: String, p: AuthServiceJWTPayload): AuthServiceJWTPayload =
    p.copy(participantId = Some(id))

  protected def forApplicationId(id: String, p: AuthServiceJWTPayload): AuthServiceJWTPayload =
    p.copy(applicationId = Some(id))

  protected def toHeader(payload: AuthServiceJWTPayload, secret: String = jwtSecret): String =
    signed(payload, secret)

  private def signed(payload: AuthServiceJWTPayload, secret: String): String =
    JwtSigner.HMAC256
      .sign(DecodedJwt(jwtHeader, AuthServiceJWTCodec.compactPrint(payload)), secret)
      .getOrElse(sys.error("Failed to generate token"))
      .value
}
