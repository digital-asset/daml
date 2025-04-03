// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import cats.syntax.option.*
import cats.syntax.parallel.*
import com.digitalasset.canton.HasTempDirectory
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{
  PostgresDumpRestore,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.client.ReplayAction.{SequencerEvents, SequencerSends}
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig}
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeBootstrap
import com.digitalasset.canton.util.FutureInstances.*
import org.scalatest.concurrent.PatienceConfiguration

import java.nio.file.Files
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class RecordReplayIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasTempDirectory
    with HasCycleUtils {

  private val postgresPlugin = new UsePostgres(loggerFactory)
  registerPlugin(postgresPlugin)
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private val postgresDumpRestore = PostgresDumpRestore(postgresPlugin, forceLocal = false)

  private val sequencerDumpFilename: String = "sequencer.tar"
  private val mediatorDumpFilename: String = "mediator.tar"
  private val participantDumpFilename: String = "participant1.tar"

  private val NumberOfRecordedWorkflows = 3 // Choosing a small value for better test performance.

  // max time replaying events should take
  private val SequencerReplayTimeout = PatienceConfiguration.Timeout(1.minute)
  // duration to observe no activity to assume that all sequencer events have been replayed
  private val SequencerReplayIdleDuration = 5.seconds
  // how many send requests should we permit sending in parallel when replaying
  private val SendReplayParallelism = 10

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  "replaying events and submission requests is functionally working" in { implicit env =>
    import env.*

    // Retrieving the mediator ID whilst the node is online.
    val mediatorId = mediator1.id

    withClue("Initialize the database") {
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.dars.upload(CantonExamplesPath) // upload before taking a dump
      participant1.health.ping(participant1)

      participant1.stop()
      sequencer1.stop()
      mediator1.stop()
    }

    withClue("Take a db dump") {
      postgresDumpRestore
        .saveDump(sequencer1, tempDirectory.toTempFile(sequencerDumpFilename))
        .futureValue
      postgresDumpRestore
        .saveDump(mediator1, tempDirectory.toTempFile(mediatorDumpFilename))
        .futureValue
      postgresDumpRestore
        .saveDump(participant1, tempDirectory.toTempFile(participantDumpFilename))
        .futureValue
    }

    withClue("Record some events") {
      MediatorNodeBootstrap.recordSequencerInteractions.set { case `mediatorId` =>
        RecordingConfig(tempDirectory.path)
      }

      sequencer1.start()
      mediator1.start()
      participant1.start()

      participant1.underlying.value.recordSequencerInteractions
        .set(Some(RecordingConfig(tempDirectory.path)))

      participant1.synchronizers.reconnect_all()

      // We're not using pings because the PingService would react to them during the replay and the reactions would time out.
      clue("recording a couple of transactions") {
        for (i <- 0 until NumberOfRecordedWorkflows) {
          runCycle(participant1.adminParty, participant1, participant1, commandId = s"cycle-$i")
        }
      }

      participant1.synchronizers.disconnect(daName)

      withClue("Unable to find the first recording.") {
        val filesInTempDirectory = Files.list(tempDirectory.path).iterator().asScala
        val submissionsAndEvents = filesInTempDirectory.filter(path =>
          path.toString.endsWith("submissions") || path.toString.endsWith("events")
        )

        submissionsAndEvents should have size 4 // mediator and participant both have files for: a db dump, .sends and .events
      }

      participant1.underlying.value.recordSequencerInteractions.set(None)
      participant1.stop()
      sequencer1.stop()
      mediator1.stop()
      MediatorNodeBootstrap.recordSequencerInteractions.set(PartialFunction.empty)
    }

    withClue("Restore the db dump") {
      postgresDumpRestore
        .restoreDump(sequencer1, (tempDirectory.directory / sequencerDumpFilename).path)
        .futureValue
      postgresDumpRestore
        .restoreDump(mediator1, (tempDirectory.directory / mediatorDumpFilename).path)
        .futureValue
      postgresDumpRestore
        .restoreDump(participant1, (tempDirectory.directory / participantDumpFilename).path)
        .futureValue
    }

    withClue("Replay participant events") {
      // The sequencer1 needs to be online, because the participant GrpcSynchronizerRegistry calls SequencerConnectService.handshake.
      sequencer1.start()
      mediator1.start()
      participant1.start()

      val txOffsetBeforeReplay = participant1.ledger_api.state.end()

      participant1.underlying.value.replaySequencerConfig
        .set(Some(ReplayConfig(tempDirectory.path, SequencerEvents)))

      participant1.synchronizers.reconnect_all()

      // Wait until all sequencer events have been output as transactions through the ledger API.
      eventually() {
        participant1.ledger_api.updates
          .flat(Set(participant1.adminParty), 2 * NumberOfRecordedWorkflows, txOffsetBeforeReplay)
          .size should be >= 2 * NumberOfRecordedWorkflows
      }

      participant1.stop()
      sequencer1.stop()
      mediator1.stop()
    }

    withClue("Restore the db dump again") {
      postgresDumpRestore
        .restoreDump(sequencer1, (tempDirectory.directory / sequencerDumpFilename).path)
        .futureValue
      postgresDumpRestore
        .restoreDump(mediator1, (tempDirectory.directory / mediatorDumpFilename).path)
        .futureValue
      postgresDumpRestore
        .restoreDump(participant1, (tempDirectory.directory / participantDumpFilename).path)
        .futureValue
    }

    withClue("Replay the sequencer submissions from the participant and mediator") {
      val mediatorSendReplayConfig = {
        val replaySendsConfig = SequencerSends(usePekko = true)
        MediatorNodeBootstrap.replaySequencerConfig.set { case `mediatorId` =>
          ReplayConfig(tempDirectory.path, replaySendsConfig)
        }
        sequencer1.start()
        mediator1.start()
        replaySendsConfig
      }

      val participantSendReplayConfig = {
        val replaySendsConfig = SequencerSends(usePekko = true)

        participant1.start()
        participant1.underlying.value.replaySequencerConfig
          .set(ReplayConfig(tempDirectory.path, replaySendsConfig).some)

        // The sequencer needs to be online here, because the GrpcSynchronizerRegistry calls SequencerConnectService.handshake.
        participant1.synchronizers.reconnect_all()

        replaySendsConfig
      }

      // wait for the nodes to fully initialize and publish the transport for us to manipulate
      val transports = List(mediatorSendReplayConfig, participantSendReplayConfig)
        .parTraverse(_.transport)
        .futureValue(SequencerReplayTimeout)

      // read all events that were previously sent
      val startingtimestamps = transports
        .parTraverse(_.waitForIdle(SequencerReplayIdleDuration).map(_.finishedAtTimestamp))
        .futureValue(SequencerReplayTimeout)

      // start an idleness monitor now before we've started replaying any events
      val idlenessMonitorsF =
        transports.zip(startingtimestamps).map { case (transport, startingTimestamp) =>
          transport.waitForIdle(SequencerReplayIdleDuration, startingTimestamp)
        }

      // replay sends and wait for events to stop being received
      val eventsReceived = transports
        .zip(idlenessMonitorsF)
        .parTraverse { case (transport, idlenessMonitorF) =>
          for {
            // throw our recorded sends at the sequencer
            _ <- transport.replay(SendReplayParallelism)
            // wait for events to stop being received
            eventsReceived <- idlenessMonitorF.map(_.totalEventsReceived)
          } yield eventsReceived
        }
        .futureValue(SequencerReplayTimeout)

      inside(eventsReceived) { case List(mediatorEventsReceived, participantEventsReceived) =>
        // we should have replayed some events
        mediatorEventsReceived should be > 0
        participantEventsReceived should be > 0
      }

    }
  }

}
