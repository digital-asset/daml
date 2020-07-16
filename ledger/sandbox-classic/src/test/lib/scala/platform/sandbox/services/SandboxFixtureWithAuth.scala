// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import java.time.{Duration, Instant}
import java.util.UUID

import com.daml.jwt.domain.DecodedJwt
import com.daml.jwt.{HMAC256Verifier, JwtSigner}
import com.daml.ledger.api.auth.{AuthServiceJWT, AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.platform.sandbox.config.SandboxConfig
import org.scalatest.Suite
import scalaz.syntax.tag.ToTagOps

trait SandboxFixtureWithAuth extends SandboxFixture { self: Suite =>

  val emptyToken = AuthServiceJWTPayload(
    ledgerId = None,
    participantId = None,
    applicationId = None,
    exp = None,
    admin = false,
    actAs = Nil,
    readAs = Nil
  )

  val adminToken: AuthServiceJWTPayload = emptyToken.copy(admin = true)

  def readOnlyToken(party: String): AuthServiceJWTPayload =
    emptyToken.copy(readAs = List(party))

  def readWriteToken(party: String): AuthServiceJWTPayload =
    emptyToken.copy(actAs = List(party))

  def expiringIn(t: Duration, p: AuthServiceJWTPayload): AuthServiceJWTPayload =
    p.copy(exp = Option(Instant.now().plusNanos(t.toNanos)))

  def forLedgerId(id: String, p: AuthServiceJWTPayload): AuthServiceJWTPayload =
    p.copy(ledgerId = Some(id))

  def forParticipantId(id: String, p: AuthServiceJWTPayload): AuthServiceJWTPayload =
    p.copy(participantId = Some(id))

  def forApplicationId(id: String, p: AuthServiceJWTPayload): AuthServiceJWTPayload =
    p.copy(applicationId = Some(id))

  override protected def config: SandboxConfig =
    super.config.copy(
      authService = Some(
        AuthServiceJWT(HMAC256Verifier(jwtSecret)
          .getOrElse(sys.error("Failed to create HMAC256 verifier")))))

  protected lazy val wrappedLedgerId = ledgerId(Some(toHeader(adminToken)))
  protected lazy val unwrappedLedgerId = wrappedLedgerId.unwrap

  private val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
  private val jwtSecret = UUID.randomUUID.toString

  private def signed(payload: AuthServiceJWTPayload, secret: String): String =
    JwtSigner.HMAC256
      .sign(DecodedJwt(jwtHeader, AuthServiceJWTCodec.compactPrint(payload)), secret)
      .getOrElse(sys.error("Failed to generate token"))
      .value

  def toHeader(payload: AuthServiceJWTPayload, secret: String = jwtSecret): String =
    signed(payload, secret)
}
