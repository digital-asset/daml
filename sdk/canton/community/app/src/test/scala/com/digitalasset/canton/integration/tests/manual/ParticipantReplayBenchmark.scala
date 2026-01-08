// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import better.files.File
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.sequencing.client.transports.replay.ReplayingEventsSequencerClientTransport
import com.digitalasset.canton.sequencing.client.{ReplayAction, ReplayConfig}
import monocle.macros.syntax.lens.*

import java.nio.file.Path
import java.time.{Duration as JDuration, Instant}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.blocking
import scala.concurrent.duration.*

/** Replay participant events recorded with [[PerformanceRunnerRecorder]].
  */
class ParticipantReplayBenchmark extends CommunityIntegrationTest with SharedEnvironment {

  private lazy val SourceDirectory: File = File(sys.env("RECORDINGS_DIR"))

  /** Overall timeout for test phase */
  private lazy val TestTimeout: FiniteDuration =
    FiniteDuration(sys.env("RECORDING_DURATION").toLong, sys.env("RECORDING_DURATION_UNIT")) * 4

  /** Poll interval to check for termination */
  private lazy val MaxPollInterval: FiniteDuration = 5.seconds

  /** If the system does not emit transactions during the given time, the test is considered
    * finished.
    */
  private lazy val FinishIfIdleDuring: FiniteDuration = 5.seconds

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .fromFiles(
        File(sys.env("CONFIG_DIR")) / "participants.conf",
        File(sys.env("CONFIG_DIR")) / sys.env("DB_TYPE") / "_persistence.conf",
      )
      .clearConfigTransforms()
      .addConfigTransforms(
        _.focus(_.parameters.timeouts.console.bounded)
          .replace(NonNegativeDuration.tryFromDuration(10.minutes))
      )
      .withManualStart // manual start, as an automatic start would attempt to reconnect to the synchronizers while they are not yet started
      .withSetup { implicit env =>
        import env.*

        utils.retry_until_true(timeout = NonNegativeDuration.tryFromDuration(10.minutes)) {
          health
            .status()
            .unreachableSequencers
            .isEmpty && health.status().unreachableMediators.isEmpty
        }

        nodes.local.start()
      }

  private def replayEvents(replayDirectory: Path, overallTimeout: FiniteDuration)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val lastTransactionTs = new AtomicReference(Instant.MAX)

    ReplayingEventsSequencerClientTransport.replayStatistics shouldBe empty

    // Kick-off the replay
    participants.local.foreach { p =>
      p.underlying.value.replaySequencerConfig
        .set(Some(ReplayConfig(replayDirectory, ReplayAction.SequencerEvents)))
      p.synchronizers.reconnect_all()
    }

    // Start measuring
    val measurements = participants.all.map { p =>
      val parties = p.parties.list().map(_.party)
      p.ledger_api.updates.start_measuring(
        parties.toSet,
        "canton.transactions-emitted",
        _ => lastTransactionTs.set(Instant.now()),
      )
    }

    val deadline = overallTimeout.fromNow

    // Wait until all events have been replayed
    for (_ <- participants.local.indices) {
      blocking {
        val timeout = deadline.timeLeft
        ReplayingEventsSequencerClientTransport.replayStatistics.poll(timeout.length, timeout.unit)
      }
    }

    // Wait until transaction stream is idle.
    eventually(deadline.timeLeft.max(Duration.Zero), MaxPollInterval) {
      JDuration
        .between(lastTransactionTs.get(), Instant.now)
        .getSeconds should be >= FinishIfIdleDuring.toSeconds
    }

    participants.local.foreach { p =>
      p.synchronizers.list_connected().foreach { item =>
        p.synchronizers.disconnect(item.synchronizerAlias.unwrap)
      }
    }

    measurements.foreach(_.close())
  }

  "Test" in { implicit env =>
    replayEvents((SourceDirectory / "test").path, TestTimeout)
  }
}
