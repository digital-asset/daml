// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.lf.engine.script.{ApiParameters, Participants, Runner, ScriptConfig}
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.Suite

import java.io.File
import scala.concurrent.ExecutionContext

trait SandboxParticipantFixture
    extends AbstractScriptTest
    with SandboxFixture
    with SandboxBackend.Postgresql
    with AkkaBeforeAndAfterAll {
  self: Suite =>
  private implicit val ec: ExecutionContext = system.dispatcher
  def participantClients(
      maxInboundMessageSize: Int = ScriptConfig.DefaultMaxInboundMessageSize,
      tlsConfiguration: TlsConfiguration = TlsConfiguration.Empty.copy(enabled = false),
  ) =
    Runner.connect(
      Participants(
        default_participant = Some(
          ApiParameters(
            host = "localhost",
            port = serverPort.value,
            access_token = None,
            application_id = None,
          )
        ),
        party_participants = Map.empty,
        participants = Map.empty,
      ),
      tlsConfig = tlsConfiguration,
      maxInboundMessageSize = maxInboundMessageSize,
    )

  override def config = super.config.copy(
    participants = singleParticipant(
      ApiServerConfig.copy(
        timeProviderType = timeMode match {
          case ScriptTimeMode.Static => TimeProviderType.Static
          case ScriptTimeMode.WallClock => TimeProviderType.WallClock
        }
      )
    )
  )

  protected def stableDarFile = new File(rlocation("daml-script/test/script-test.dar"))
  protected def devDarFile = new File(rlocation("daml-script/test/script-test-1.dev.dar"))

  override def packageFiles: List[File] =
    List(stableDarFile, devDarFile)
}
