// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.File

import com.daml.lf.engine.script.{ApiParameters, Participants, Runner, RunnerConfig}
import com.daml.platform.sandboxnext.SandboxNextFixture
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.Suite
import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.testing.postgresql.PostgresAroundAll

import scala.concurrent.ExecutionContext

trait SandboxParticipantFixture
    extends AbstractScriptTest
    with SandboxNextFixture
    with PostgresAroundAll
    with AkkaBeforeAndAfterAll {
  self: Suite =>
  private implicit val ec: ExecutionContext = system.dispatcher
  def participantClients(
      maxInboundMessageSize: Int = RunnerConfig.DefaultMaxInboundMessageSize,
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
  override protected def config = super.config.copy(
    timeProviderType = Some(timeMode match {
      case ScriptTimeMode.Static => TimeProviderType.Static
      case ScriptTimeMode.WallClock => TimeProviderType.WallClock
    }),
    jdbcUrl = Some(
      postgresDatabase.url
    ),
  )

  protected def stableDarFile = new File(rlocation("daml-script/test/script-test.dar"))
  protected def devDarFile = new File(rlocation("daml-script/test/script-test-1.dev.dar"))

  override def packageFiles: List[File] =
    List(stableDarFile, devDarFile)
}
