// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.data.PruningSchedule
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, PositiveDurationSeconds}
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.BackgroundWorkloadRunner
import com.digitalasset.canton.scheduler.IgnoresTransientSchedulerErrors
import com.digitalasset.canton.time.NonNegativeFiniteDuration

import java.time.Duration as JDuration

class ScheduledParticipantPruningTestPostgres extends ScheduledParticipantPruningTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

abstract class ScheduledParticipantPruningTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with ConfiguresScheduledPruning
    with IgnoresTransientSchedulerErrors
    with BackgroundWorkloadRunner {

  private val reconciliationInterval = JDuration.ofSeconds(1)
  private val confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)
  private val mediatorReactionTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)

  private val internalPruningBatchSize =
    PositiveInt.tryCreate(5) // small enough to exercise batching of prune requests
  private val maxDedupDuration =
    JDuration.ofSeconds(1) // Low so that we don't delay pruning unnecessarily

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.updatePruningBatchSize(internalPruningBatchSize),
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
      )
      .withSetup { env =>
        import env.*
        sequencer1.topology.synchronizer_parameters.propose_update(
          daId,
          _.update(
            confirmationResponseTimeout = confirmationResponseTimeout.toConfig,
            mediatorReactionTimeout = mediatorReactionTimeout.toConfig,
            reconciliationInterval = PositiveDurationSeconds(reconciliationInterval),
          ),
        )
      }

  "Able to run scheduled participant pruning via ledger api" in { implicit env =>
    verifyScheduledPruning(pruneInternallyOnly = false)
  }

  "Able to run scheduled participant internal-only pruning" in { implicit env =>
    verifyScheduledPruning(pruneInternallyOnly = true)
  }

  def verifyScheduledPruning(
      pruneInternallyOnly: Boolean
  )(implicit env: TestConsoleEnvironment): Any = {
    import env.*
    Seq(participant1, participant2).foreach { p =>
      p.synchronizers.connect_local(sequencer1, alias = daName)
      p.dars.upload(CantonTestsPath)
    }
    eventually()(assert(Seq(participant1, participant2).forall(_.synchronizers.active(daName))))

    // should fail because no schedule to update has been configured yet
    Seq[(String, LocalParticipantReference => Unit)](
      "cron" -> { _.pruning.set_cron("* * * * * ? *") },
      "max_duration" -> {
        _.pruning.set_max_duration(config.PositiveDurationSeconds.ofSeconds(100L))
      },
      "retention" -> { _.pruning.set_retention(config.PositiveDurationSeconds.ofSeconds(100L)) },
    ).foreach { case (field, update) =>
      loggerFactory.assertLogs(
        a[CommandFailure] shouldBe thrownBy(update(participant1)),
        _.commandFailureMessage should include(
          s"GrpcClientError: INVALID_ARGUMENT/Attempt to update $field of a schedule that has not been previously configured. Use set_schedule or set_participant_schedule instead."
        ),
      )
    }

    val pruningIntervalP1 = 2
    val pruningIntervalP2 = 3
    val retentionP1 = 2
    val retentionP2 = 3

    // The larger the maxDurations, the more time the pruning scheduler has to prune.
    // However since the check at the bottom of this test is conservative assuming only
    // one pruning operation per "window", these values are not part of the test criteria
    // below.
    val maxDurationP1 = 1
    val maxDurationP2 = 2

    ignoreTransientSchedulerErrors("ParticipantPruningScheduler") {
      Seq(
        (participant1, pruningIntervalP1, maxDurationP1, retentionP1),
        (participant2, pruningIntervalP2, maxDurationP2, retentionP2),
      )
        .foreach { case (p, interval, maxDuration, retention) =>
          val pruningSchedule = PruningSchedule(
            s"/$interval * * * * ? *",
            config.PositiveDurationSeconds.ofSeconds(maxDuration.toLong),
            config.PositiveDurationSeconds.ofSeconds(retention.toLong),
          )
          if (pruneInternallyOnly) {
            p.pruning.set_participant_schedule(
              pruningSchedule.cron,
              pruningSchedule.maxDuration,
              pruningSchedule.retention,
              pruneInternallyOnly = true,
            )
            val participantSchedule = p.pruning.get_participant_schedule()
            participantSchedule.value.pruneInternallyOnly shouldBe true
          } else {
            setAndVerifyPruningSchedule(p.pruning, pruningSchedule)
          }
        }

      val secondsToRunTest = 20

      val (pruningStatsP1, pruningStatsP2) =
        withWorkload(NonEmpty.mk(Seq, participant1, participant2)) {
          // Check how often pruning was performed by repeatedly reading the ledger "begin" offset.
          // An increase of the ledger begin is taken as a sign that pruning has been performed.
          val (offsetsP1, offsetsP2) = (for { _ <- 1 to secondsToRunTest } yield {
            Threading.sleep(1000)

            def ledgerBeginOffset(p: LocalParticipantReference): Long =
              p.testing.state_inspection.prunedUptoOffset
                .fold(0L)(_.unwrap)

            val offset1 = ledgerBeginOffset(participant1)
            val offset2 = ledgerBeginOffset(participant2)
            // Log intermediate offsets for debugging
            logger.info(s"Begin offsets: $offset1, $offset2")
            (offset1, offset2)
          }).unzip
          // count how many times each participant was pruned

          Seq(participant1, participant2).foreach(_.pruning.clear_schedule())

          (countPruning(offsetsP1), countPruning(offsetsP2))
        }

      // Check that background pruning has run often enough
      logger.info(s"P1 pruned $pruningStatsP1 times and P2 pruned $pruningStatsP2 times")

      // To avoid making this test flaky, allow for up to half the prunings to be skipped, e.g due to slow dbs
      val toleranceToAvoidTestFlakiness = 2

      pruningStatsP1 should be > (secondsToRunTest - retentionP1) / pruningIntervalP1 / toleranceToAvoidTestFlakiness
      pruningStatsP2 should be > (secondsToRunTest - retentionP2) / pruningIntervalP2 / toleranceToAvoidTestFlakiness
    }
  }

  // From a list of beginning offset, infer the number of times pruning happened
  private def countPruning(offsets: Seq[Long]): Int = offsets
    .foldLeft((0, 1L)) { case ((pruningCount, previousOffset), nextOffset) =>
      val newPruningCount = pruningCount + (if (nextOffset > previousOffset) 1 else 0)
      (newPruningCount, nextOffset)
    }
    ._1
}
