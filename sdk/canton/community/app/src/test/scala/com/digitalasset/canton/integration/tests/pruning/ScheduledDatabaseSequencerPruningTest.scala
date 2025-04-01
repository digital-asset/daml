// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.data.PruningSchedule
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.SequencerReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.util.BackgroundWorkloadRunner
import com.digitalasset.canton.integration.{CommunityIntegrationTest, ConfigTransform, *}
import com.digitalasset.canton.scheduler.IgnoresTransientSchedulerErrors
import com.digitalasset.canton.time.NonNegativeFiniteDuration

import scala.util.chaining.*

// TODO(#16089) enable one of these tests after sequencer unification
//class ScheduledDatabaseSequencerPruningTestDefault extends ScheduledDatabaseSequencerPruningTest {
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//}

//class ScheduledDatabaseSequencerPruningTestPostgres extends ScheduledDatabaseSequencerPruningTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
//}

abstract class ScheduledDatabaseSequencerPruningTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with ConfiguresScheduledPruning
    with IgnoresTransientSchedulerErrors
    with BackgroundWorkloadRunner
    with CanMeasureDatabaseSequencerPruning {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(fasterPruningConfigTransforms*)

  // The larger the maxDuration, the more time the pruning scheduler has to prune.
  // However since the check at the bottom of this test is conservative assuming only
  // one pruning operation per "window", retention and maxDuration are not part of the
  // test criteria below.
  private val pruningInterval = 2

  // TODO(#16089) enable after sequencer unification
  "Able to configure scheduled sequencer pruning and observe scheduled pruning" ignore {
    implicit env =>
      import env.*
      Seq(participant1, participant2).foreach { p =>
        p.synchronizers.connect_local(sequencer1, alias = daName)
        p.dars.upload(CantonTestsPath)
      }
      eventually()(assert(Seq(participant1, participant2).forall(_.synchronizers.active(daName))))

      ignoreTransientSchedulerErrors("DatabaseSequencerPruningScheduler") {
        setAndVerifyPruningSchedule(
          sequencer1.pruning,
          PruningSchedule(
            s"/$pruningInterval * * * * ? *",
            config.PositiveDurationSeconds.ofSeconds(1L),
            config.PositiveDurationSeconds.ofSeconds(5L),
          ),
        )

        assertPruningOngoing(
          sequencer1,
          pruningInterval,
          secondsToRunTest = 20,
          clearScheduleAtEnd = true,
        )
      }
  }
}

private[pruning] trait CanMeasureDatabaseSequencerPruning {
  this: BackgroundWorkloadRunner & ConfiguresScheduledPruning & CommunityIntegrationTest =>

  // pick a smaller batch size but one still large enough not to be repeatedly "rounded down" by
  // checkpoint constraint as the sequencer refuses to prune partial checkpoints.
  private val internalPruningBatchSize =
    PositiveInt.tryCreate(100)

  // Lower the interval of a number of configs enable more instantaneous sequencer pruning
  private val intervalForFrequentPruning = NonNegativeFiniteDuration.tryOfMillis(500)

  protected val fasterPruningConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateDatabaseSequencerPruningBatchSize(internalPruningBatchSize),
    // Have synchronizer TopologyManagementComponents read their respective sequencer clients frequently
    ConfigTransforms.updateSynchronizerTimeTrackerInterval(intervalForFrequentPruning),
    // Have the sequencer generate frequent checkpoints to enable more granular pruning
    ConfigTransforms.updateSequencerCheckpointInterval(intervalForFrequentPruning),
    // Have the synchronizer and participant sequencer clients acknowledge frequently
    ConfigTransforms.updateSequencerClientAcknowledgementInterval(intervalForFrequentPruning),
  )

  protected def assertPruningOngoing(
      sequencer: SequencerReference,
      pruningInterval: Int,
      secondsToRunTest: Int,
      clearScheduleAtEnd: Boolean,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val pruningDoneCount = withWorkload(NonEmpty.mk(Seq, participant1, participant2)) {
      // Check how often pruning was performed by repeatedly reading the ledger "begin" timestamp.
      // An increase of the ledger begin is taken as a sign that pruning has been performed.
      val timestamps = for { _ <- 1 to secondsToRunTest } yield {
        Threading.sleep(1000)
        val ts = sequencer.pruning
          .locate_pruning_timestamp()
          .getOrElse(CantonTimestamp.Epoch)
          .tap(timestamp =>
            // Log intermediate timestamps for debugging
            logger.info(s"Begin timestamp: $timestamp")
          )
        val status = sequencer.pruning.status()
        // useful if test fails to see which sequencer client has delayed pruning
        logger.info(s"Pruning status: $status")
        ts
      }

      if (clearScheduleAtEnd) {
        sequencer.pruning.clear_schedule()
      }

      countPrunings(CantonTimestamp.Epoch)(timestamps)
    }

    // Check that background pruning has run often enough
    logger.info(s"Sequencer pruned $pruningDoneCount times")

    // To avoid making this test flaky, allow for up to half the prunings to be skipped, e.g due to slow dbs
    val toleranceToAvoidTestFlakiness = 2

    pruningDoneCount should be > secondsToRunTest / pruningInterval / toleranceToAvoidTestFlakiness
  }
}
