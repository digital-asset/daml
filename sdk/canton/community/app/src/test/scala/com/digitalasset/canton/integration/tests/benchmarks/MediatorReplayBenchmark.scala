// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.benchmarks

import better.files.File
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.integration.plugins.{
  PostgresDumpRestore,
  UseBftSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.manual.ReplayTestCommon.{
  configTransforms,
  dumpPathOfNode,
  testDirectory,
  warmupDirectory,
}
import com.digitalasset.canton.integration.tests.manual.{
  BftSequencerPerformanceRunnerRecorder,
  PerformanceRunnerRecorder,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.sequencing.client.ReplayAction.SequencerEvents
import com.digitalasset.canton.sequencing.client.ReplayConfig
import com.digitalasset.canton.sequencing.client.transports.replay.ReplayingEventsSequencerClientTransport
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeBootstrap
import com.digitalasset.canton.topology.MediatorId
import com.digitalasset.canton.util.ShowUtil.*
import org.scalatest.concurrent.PatienceConfiguration

import java.nio.file.Path
import java.time.{Duration as JDuration, Instant}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*

/** Replay participant events recorded with [[PerformanceRunnerRecorder]].
  */
class MediatorReplayBenchmark
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext {

  private val SourceDirectory: File = File("replay")

  /** Overall timeout for a replay */
  private val InitTimeout: FiniteDuration = 2.minutes

  /** Overall timeout for a replay */
  private val Timeout: FiniteDuration = 5.minutes

  /** How frequently should we check if the idleness future has completed */
  private val IdleCheckInterval: FiniteDuration = 1.second

  /** Callback for the summary report */
  private val reportSummary: String => Unit = println(_)

  private val postgresPlugin = new UsePostgres(
    loggerFactory,
    customDbNames = Some((identity, "_replay_tests")),
  )
  registerPlugin(postgresPlugin)

  private val performanceRunnerRecorder: PerformanceRunnerRecorder =
    new BftSequencerPerformanceRunnerRecorder()
  registerPlugin(new UseBftSequencer(loggerFactory))

  // Use local tools by default as the dump files are not copied into the container.
  // Otherwise, the tests would fail with "No such file or directory".
  private lazy val postgresDumpRestore = PostgresDumpRestore(postgresPlugin, forceLocal = true)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P0S1M1_Manual
      .clearConfigTransforms() // to disable globally unique ports
      .addConfigTransforms(configTransforms()*)
      .addConfigTransform(
        // TODO(i26481): Enable new connection pool (test uses replay subscriptions)
        ConfigTransforms.disableConnectionPool
      )

  override def afterAll(): Unit = {
    // ensure that we don't leave the replay config on the mediator class that could be erroneously picked up by other tests
    MediatorNodeBootstrap.replaySequencerConfig.set(PartialFunction.empty)
    super.afterAll()
  }

  private def replayEvents(replayDirectory: Path)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    // Make sure we don't overwrite recordings
    MediatorNodeBootstrap.recordSequencerInteractions.set(PartialFunction.empty)

    MediatorNodeBootstrap.replaySequencerConfig.set { case MediatorId(_) =>
      ReplayConfig(replayDirectory, SequencerEvents)
    }

    // Kick-off the replay
    sequencer1.start()
    mediator1.start()

    val lastFlushTs = new AtomicReference(Instant.now())
    val sequencerClient =
      mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator.sequencerClient

    // Wait until endTime no longer gets updated.
    eventually(Timeout, 1.second) { // Choose a low poll interval to get accurate measurements.
      sequencerClient.flush().futureValueUS
      lastFlushTs.set(Instant.now())

      ReplayingEventsSequencerClientTransport.replayStatistics should not be empty withClue
        "Missing replay statistics. Apparently, the sequencer client has not yet finished replaying events."
    }

    val ReplayingEventsSequencerClientTransport.ReplayStatistics(
      inputPath,
      numberOfEvents,
      startTime,
      handoverDuration,
    ) =
      Option(
        ReplayingEventsSequencerClientTransport.replayStatistics.poll(1, TimeUnit.SECONDS)
      ).value

    ReplayingEventsSequencerClientTransport.replayStatistics shouldBe empty withClue "Too many replay statistics. The test setup seems broken."

    val duration = JDuration.between(startTime.toInstant, lastFlushTs.get())

    sequencers.local.stop()
    mediators.local.stop()

    reportSummary(show"""Replayed $numberOfEvents events within $duration.
            |Duration of handover: $handoverDuration
            |Input path: ${inputPath.toString.doubleQuoted}""".stripMargin)
  }

  "Run the performance runner recorder" in { _ =>
    performanceRunnerRecorder.execute(durations = true, fullstacks = true)
  }

  "Initialize the DB" in { implicit env =>
    import env.*

    Seq[LocalInstanceReference](sequencer1, mediator1).foreach { node =>
      postgresDumpRestore
        .restoreDump(node, dumpPathOfNode(node.name, SourceDirectory))
        .futureValue(
          PatienceConfiguration.Timeout(InitTimeout),
          PatienceConfiguration.Interval(IdleCheckInterval),
        )
    }
  }

  "Warmup" in { implicit env =>
    replayEvents(warmupDirectory(SourceDirectory).path)
  }

  "Test" in { implicit env =>
    println("Running Mediator Replay Test")
    replayEvents(testDirectory(SourceDirectory).path)
  }
}
