// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, OwnedResource, SuiteResource}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.on.memory.Owner
import com.daml.ledger.participant.state.kvutils.app.{
  ParticipantConfig,
  ParticipantIndexerConfig,
  ParticipantRunMode,
}
import com.daml.ledger.participant.state.kvutils.{app => kvutils}
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.data.Ref
import com.daml.lf.engine.script._
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.ports.Port
import org.scalatest.Suite

import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext

trait MultiParticipantFixture
    extends AbstractScriptTest
    with SuiteResource[(Port, Port)]
    with AkkaBeforeAndAfterAll {
  self: Suite =>
  private def darFile = Paths.get(rlocation("daml-script/test/script-test.dar"))
  private val tmpDir = Files.createTempDirectory("testMultiParticipantFixture")
  private val participant1Portfile = tmpDir.resolve("participant1-portfile")
  private val participant2Portfile = tmpDir.resolve("participant2-portfile")

  override protected def afterAll(): Unit = {
    Files.delete(participant1Portfile)
    Files.delete(participant2Portfile)
    super.afterAll()

  }

  private def readPortfile(f: Path): Port = {
    Port(Integer.parseInt(Files.readAllLines(f).stream.collect(Collectors.joining("\n"))))
  }

  private val participantId1 = Ref.ParticipantId.assertFromString("participant1")
  private val participant1 = ParticipantConfig(
    mode = ParticipantRunMode.Combined,
    participantId = participantId1,
    shardName = None,
    address = Some("localhost"),
    port = Port.Dynamic,
    portFile = Some(participant1Portfile),
    serverJdbcUrl = ParticipantConfig.defaultIndexJdbcUrl(participantId1),
    indexerConfig = ParticipantIndexerConfig(
      allowExistingSchema = false
    ),
  )
  private val participantId2 = Ref.ParticipantId.assertFromString("participant2")
  private val participant2 = ParticipantConfig(
    mode = ParticipantRunMode.Combined,
    participantId = participantId2,
    shardName = None,
    address = Some("localhost"),
    port = Port.Dynamic,
    portFile = Some(participant2Portfile),
    serverJdbcUrl = ParticipantConfig.defaultIndexJdbcUrl(participantId2),
    indexerConfig = ParticipantIndexerConfig(
      allowExistingSchema = false
    ),
  )
  override protected lazy val suiteResource = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (Port, Port)](
      for {
        _ <- Owner(
          kvutils.Config
            .createDefault(())
            .copy(
              participants = Seq(participant1, participant2),
              archiveFiles = Seq(darFile),
            )
        )
      } yield (readPortfile(participant1Portfile), readPortfile(participant2Portfile)),
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }

  def participantClients() = {
    implicit val ec: ExecutionContext = system.dispatcher
    val params = Participants(
      None,
      Seq(
        (Participant("one"), ApiParameters("localhost", suiteResource.value._1.value, None, None)),
        (Participant("two"), ApiParameters("localhost", suiteResource.value._2.value, None, None)),
      ).toMap,
      Map.empty,
    )
    Runner.connect(
      params,
      tlsConfig = TlsConfiguration(false, None, None, None),
      maxInboundMessageSize = RunnerConfig.DefaultMaxInboundMessageSize,
    )
  }

  override def timeMode: ScriptTimeMode = ScriptTimeMode.WallClock
}
