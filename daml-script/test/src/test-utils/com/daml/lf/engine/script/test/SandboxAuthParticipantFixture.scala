// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.bazeltools.BazelRunfiles._
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, CustomDamlJWTPayload}
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import com.daml.lf.engine.script._
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.platform.apiserver.AuthServiceConfig.UnsafeJwtHmac256
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.Suite
import scalaz.syntax.tag._
import scalaz.{-\/, \/-}

import java.io.File
import scala.concurrent.ExecutionContext

trait SandboxAuthParticipantFixture
    extends AbstractScriptTest
    with SandboxFixture
    with SandboxBackend.Postgresql
    with AkkaBeforeAndAfterAll {
  self: Suite =>
  private implicit val ec: ExecutionContext = system.dispatcher
  def participantClients(parties: List[String], admin: Boolean) =
    Runner.connect(
      Participants(
        default_participant = Some(
          ApiParameters(
            host = "localhost",
            port = serverPort.value,
            access_token = Some(getToken(parties, admin)),
            application_id = Some(appId),
          )
        ),
        party_participants = Map.empty,
        participants = Map.empty,
      ),
      tlsConfig = TlsConfiguration(false, None, None, None),
      maxInboundMessageSize = ScriptConfig.DefaultMaxInboundMessageSize,
    )

  private val secret = "secret"

  override def config = super.config.copy(
    participants = singleParticipant(
      ApiServerConfig.copy(
        timeProviderType = TimeProviderType.WallClock
      ),
      authentication = UnsafeJwtHmac256(secret),
    )
  )
  override def timeMode = ScriptTimeMode.WallClock

  private val appId = ApplicationId("daml-script-test")

  def getToken(parties: List[String], admin: Boolean): String = {
    val payload = CustomDamlJWTPayload(
      ledgerId = None,
      participantId = None,
      exp = None,
      // Set the application id to make sure it is set correctly.
      applicationId = Some(appId.unwrap),
      actAs = parties,
      admin = admin,
      readAs = List(),
    )
    val header = """{"alg": "HS256", "typ": "JWT"}"""
    val jwt = DecodedJwt[String](header, AuthServiceJWTCodec.writeToString(payload))
    JwtSigner.HMAC256.sign(jwt, secret) match {
      case -\/(e) => throw new IllegalStateException(e.toString)
      case \/-(a) => a.value
    }
  }

  override def packageFiles: List[File] =
    List(new File(rlocation("daml-script/test/script-test.dar")))
}
