// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.{File, FileInputStream}
import com.daml.lf.engine.script.{ApiParameters, Participants, Runner, ScriptConfig}
import com.daml.platform.sandbox.{SandboxBackend, SandboxRequiringAuthorizationFuns}
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.Suite
import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.sandbox.SandboxOnXForTest.ParticipantId
import com.daml.lf.engine.script.Runner.connectApiParameters
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.platform.sandbox.services.TestCommands
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

trait SandboxParticipantFixture
    extends AbstractScriptTest
    with SandboxFixture
    with SandboxBackend.Postgresql
    with SandboxRequiringAuthorizationFuns
    with TestCommands
    with AkkaBeforeAndAfterAll {
  self: Suite =>
  private implicit val ec: ExecutionContext = system.dispatcher
  def participantClients(
      maxInboundMessageSize: Int = ScriptConfig.DefaultMaxInboundMessageSize,
      tlsConfiguration: TlsConfiguration = TlsConfiguration.Empty.copy(enabled = false),
  ) = {
    val apiParameters = ApiParameters(
      host = "localhost",
      port = serverPort.value,
      access_token = None,
      application_id = None,
    )
    for {
      participantClients <- Runner
        .connect(
          Participants(
            default_participant = Some(apiParameters),
            party_participants = Map.empty,
            participants = Map.empty,
          ),
          tlsConfig = tlsConfiguration,
          maxInboundMessageSize = maxInboundMessageSize,
        )
      ledgerClient <- connectApiParameters(
        apiParameters,
        tlsConfiguration,
        maxInboundMessageSize,
      )
      _ <- Future.sequence(packageFiles.map { dar =>
        ledgerClient.grpcClient.packageManagementClient
          .uploadDarFile(ByteString.readFrom(new FileInputStream(dar)))
      })
    } yield {
      participantClients
    }
  }

  override def config = super.config.copy(participants =
    Map(
      ParticipantId -> super.config
        .participants(ParticipantId)
        .copy(
          apiServer = super.config
            .participants(ParticipantId)
            .apiServer
            .copy(
              timeProviderType = timeMode match {
                case ScriptTimeMode.Static => TimeProviderType.Static
                case ScriptTimeMode.WallClock => TimeProviderType.WallClock
              }
            )
        )
    )
  )

  protected def stableDarFile = new File(rlocation("daml-script/test/script-test.dar"))
  protected def devDarFile = new File(rlocation("daml-script/test/script-test-1.dev.dar"))

  override def packageFiles: List[File] =
    List(stableDarFile, devDarFile)
}
