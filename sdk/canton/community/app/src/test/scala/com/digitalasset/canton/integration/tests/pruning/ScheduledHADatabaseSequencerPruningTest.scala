// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import cats.syntax.option.*
import com.digitalasset.canton.admin.api.client.data.PruningSchedule
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseSharedStorage,
}
import com.digitalasset.canton.integration.util.BackgroundWorkloadRunner
import com.digitalasset.canton.scheduler.IgnoresTransientSchedulerErrors
import com.digitalasset.canton.time.PositiveFiniteDuration
import monocle.macros.syntax.lens.*

import scala.util.control.NonFatal

class ScheduledHADatabaseSequencerPruningTestPostgres
    extends ScheduledHADatabaseSequencerPruningTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(
    UseSharedStorage.forSequencers(
      "sequencer1",
      Seq("sequencer2", "sequencer3", "sequencer4"),
      loggerFactory,
    )
  )
}

abstract class ScheduledHADatabaseSequencerPruningTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with ConfiguresScheduledPruning
    with IgnoresTransientSchedulerErrors
    with BackgroundWorkloadRunner
    with CanMeasureDatabaseSequencerPruning {

  // Lower the interval to enable faster sequencer exclusive storage failover
  private val intervalForFasterDbLockFailover = PositiveFiniteDuration.tryOfMillis(500)

  def increaseShutdownNetworkDurationForSequencerClient: ConfigTransform =
    _.focus(_.parameters.timeouts.processing.shutdownNetwork)
      .replace(config.NonNegativeDuration.ofSeconds(20))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S2M2
      .addConfigTransforms(
        (fasterPruningConfigTransforms :+
          // Speed up the time for exclusive storage failover to speed up test
          ConfigTransforms
            .updateSequencerExclusiveStorageFailoverInterval(
              intervalForFasterDbLockFailover
            ) :+
          // To avoid flaky warnings during failover sequencer-shutdown, give the sequencer's
          // direct/resilient subscription more than twice the normal time for the pekko-killswitch
          // to take effect.
          increaseShutdownNetworkDurationForSequencerClient)*
      )

  // The larger the maxDuration, the more time the pruning scheduler has to prune.
  // However since the check at the bottom of this test is conservative assuming only
  // one pruning operation per "window", retention and maxDuration are not part of the
  // test criteria below.
  private val pruningInterval = 2
  private val maxPruningDuration = config.PositiveDurationSeconds.ofSeconds(1L)

  // TODO(#16089) enable after sequencer unification
  "Able to bootstrap synchronizer with multiple sequencers for ha sequencer pruning test" ignore {
    implicit env =>
      import env.*

      Seq(participant1, participant2).foreach { p =>
        p.synchronizers.connect_multi(daName, Seq(sequencer1, sequencer2))
        p.dars.upload(CantonTestsPath)
      }
      eventually()(assert(Seq(participant1, participant2).forall(_.synchronizers.active(daName))))
  }

  // TODO(#16089) enable after sequencer unification
  "Able to use enterprise console pruning macro" ignore { implicit env =>
    env.pruning.clear_schedule()
  }

  // TODO(#16089) enable after sequencer unification
  "Able to configure scheduled sequencer pruning and observe scheduled pruning with database sequencer exclusive storage failovers" ignore {
    implicit env =>
      import env.*
      ignoreTransientSchedulerErrors("DatabaseSequencerPruningScheduler") {
        clue("Configure scheduled sequencer pruning") {
          val schedule = PruningSchedule(
            s"/$pruningInterval * * * * ? *",
            maxPruningDuration,
            config.PositiveDurationSeconds.ofSeconds(5L),
          )

          // Configure pruning scheduler shared by all sequencers (in same synchronizer) by trying to set_schedule on all
          // sequencers. This will only succeed on the one sequencer with write access to "exclusive storage"
          configurePruningOnAllSequencersAndReturnAdminSequencer(
            _.pruning
              .set_schedule(schedule.cron, schedule.maxDuration, schedule.retention),
            "set_schedule",
          ).discard

          // All sequencers should report the same schedule
          sequencers.local.foreach(_.pruning.get_schedule() shouldBe schedule.some)
        }

        clue("Observe the database sequencer pruning scheduler perform pruning") {
          assertPruningOngoing(
            sequencer1,
            pruningInterval,
            secondsToRunTest = 20,
            clearScheduleAtEnd = false,
          )
        }

        clue("Observe continued database sequencer pruning throughout a series of failovers") {
          val howManyFailovers = 2

          (1 to howManyFailovers).foreach { i =>
            logger.info(s"Beginning failover number $i")

            // Perform a dummy sequencer pruning schedule update (that does not modify the schedule) to
            // find out which sequencer actively owns the shared sequencer exclusive storage.
            val activeSequencer =
              configurePruningOnAllSequencersAndReturnAdminSequencer(
                _.pruning.set_max_duration(maxPruningDuration),
                "set_max_duration",
              )

            // Take down the "active" sequencer to test that another sequencer takes over the pruning responsibility.
            activeSequencer.stop()
            val otherSequencer = sequencers.local.find(_.is_running).value

            assertPruningOngoing(
              otherSequencer,
              pruningInterval,
              secondsToRunTest =
                30, // + 10 seconds to give some time to "recover" from failover and reduce flakiness
              clearScheduleAtEnd = false,
            )

            activeSequencer.start()
            logger.info(s"Successfully recovered from failover number $i")
          }
        }

        clue("After failovers, perform one final sanity check") {
          // Perform a dummy sequencer pruning schedule update (that does not modify the schedule) to
          // find out which sequencer actively owns the shared sequencer exclusive storage.
          val activeSequencer =
            configurePruningOnAllSequencersAndReturnAdminSequencer(
              _.pruning.set_max_duration(maxPruningDuration),
              "set_max_duration",
            )

          assertPruningOngoing(
            activeSequencer,
            pruningInterval,
            secondsToRunTest = 20,
            // have the active sequencer clear the schedule at the end of the test:
            clearScheduleAtEnd = true,
          )
        }
      }
  }

  /** Private helper to issue a pruning schedule modification to all sequencers. Assert that one
    * sequencer must succeed.
    *
    * Note: The risk of incurring a failover not triggered by this test (but instead e.g. the
    * database becoming unresponsive) is capped by `DbLockedConnectionPoolConfig.checkPeriod`
    * (lowered to 1/2 second to make this test take less time). Should we ever observe test
    * flakiness, we can increase the check period (along with the `secondsToRunTest` duration).
    */
  private[this] def configurePruningOnAllSequencersAndReturnAdminSequencer(
      tryConfigure: LocalSequencerReference => Unit,
      commandName: String,
  )(implicit env: TestConsoleEnvironment): LocalSequencerReference = {
    import env.*
    val sequencerWithExclusiveStorage: Option[LocalSequencerReference] =
      sequencers.local.find(
        _.health.status.successOption.exists(_.admin.acceptsAdminChanges)
      )

    sequencerWithExclusiveStorage.foreach(sequencer =>
      try {
        logger.info(s"Trying to $commandName on ${sequencer.name}")
        tryConfigure(sequencer)
        logger.info(s"Managed to $commandName on ${sequencer.name}")
      } catch {
        case NonFatal(t) =>
          logger.info(s"Failed to $commandName on ${sequencer.name}", t)
      }
    )

    sequencerWithExclusiveStorage.getOrElse(fail("Expected active sequencer but found none"))
  }

}
