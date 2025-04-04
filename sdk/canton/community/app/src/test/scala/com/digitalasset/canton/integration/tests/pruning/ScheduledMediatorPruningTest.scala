// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.data.PruningSchedule
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, PositiveDurationSeconds}
import com.digitalasset.canton.console.{CommandFailure, LocalMediatorReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.BackgroundWorkloadRunner
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.scheduler.IgnoresTransientSchedulerErrors
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.version.ProtocolVersion

import scala.util.chaining.*

class ScheduledMediatorPruningTestPostgres extends ScheduledMediatorPruningTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

abstract class ScheduledMediatorPruningTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with ConfiguresScheduledPruning
    with IgnoresTransientSchedulerErrors
    with BackgroundWorkloadRunner {

  // Lower the timeout from 30 seconds to 4 to allow the test to prune more aggressively
  // especially in the second half once the default value of 30 seconds has been reached.
  private val confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(4)

  private val internalPruningBatchSize =
    PositiveInt.tryCreate(5) // small enough to exercise batching of prune requests

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateMediatorPruningBatchSize(internalPruningBatchSize)
      )
      .withSetup { env =>
        env.sequencer1.topology.synchronizer_parameters
          .propose_update(
            env.daId,
            _.update(confirmationResponseTimeout = confirmationResponseTimeout.toConfig),
          )
      }

  private val pruningInterval = 2
  private val retention = 5

  // The larger the maxDurations, the more time the pruning scheduler has to prune.
  // However since the check at the bottom of this test is conservative assuming only
  // one pruning operation per "window", these values are not part of the test criteria
  // below.
  private val maxDuration = 1

  "Able to configure scheduled mediator pruning and observe scheduled mediator pruning" in {
    implicit env =>
      import env.*
      Seq(participant1, participant2).foreach { p =>
        p.synchronizers.connect_local(sequencer1, alias = daName)
        p.dars.upload(CantonTestsPath)
      }
      eventually()(assert(Seq(participant1, participant2).forall(_.synchronizers.active(daName))))

      // should fail because no schedule to update has been configured yet
      Seq[(String, LocalMediatorReference => Unit)](
        "cron" -> {
          _.pruning.set_cron("* * * * * ? *")
        },
        "max_duration" -> {
          _.pruning.set_max_duration(PositiveDurationSeconds.ofSeconds(100L))
        },
        "retention" -> {
          _.pruning.set_retention(PositiveDurationSeconds.ofSeconds(100L))
        },
      ).foreach { case (field, update) =>
        loggerFactory.assertLogs(
          a[CommandFailure] shouldBe thrownBy(update(mediator1)),
          _.commandFailureMessage should include(
            s"GrpcClientError: INVALID_ARGUMENT/Attempt to update $field of a schedule that has not been previously configured. Use set_schedule instead."
          ),
        )
      }

      ignoreTransientSchedulerErrors("MediatorPruningScheduler") {
        setAndVerifyPruningSchedule(
          mediator1.pruning,
          PruningSchedule(
            s"/$pruningInterval * * * * ? *",
            config.PositiveDurationSeconds.ofSeconds(maxDuration.toLong),
            config.PositiveDurationSeconds.ofSeconds(retention.toLong),
          ),
        )

        // Running > 30 seconds to give default confirmationResponseTimeout of 30 seconds to "age out" of the system.
        // In the second half, the pruning test is able to prune a lot more aggressively according to the 4 second
        // timeout set as a dynamic synchronizer parameter.
        val secondsWorstCaseUntilFirstPrune = DynamicSynchronizerParameters
          .initialValues(env.environment.clock, ProtocolVersion.latest)
          .confirmationResponseTimeout // == 30.seconds
          .unwrap
          .getSeconds
          .toInt
        val secondsToRunTest = 50

        val pruningDoneCount = withWorkload(NonEmpty.mk(Seq, participant1, participant2)) {
          // Check how often pruning was performed by repeatedly reading the ledger "begin" timestamp.
          // An increase of the ledger begin is taken as a sign that pruning has been performed.
          val timestamps = for { _ <- 1 to secondsToRunTest } yield {
            Threading.sleep(1000)
            mediator1.pruning
              .locate_pruning_timestamp()
              .getOrElse(CantonTimestamp.Epoch)
              .tap(timestamp =>
                // Log intermediate timestamps for debugging
                logger.info(s"Begin timestamp: $timestamp")
              )
          }

          mediator1.pruning.clear_schedule()

          countPrunings(CantonTimestamp.Epoch)(timestamps)
        }

        // Check that background pruning has run often enough
        logger.info(s"Synchronizer da pruned $pruningDoneCount times")

        // To avoid making this test flaky, allow for up to half the prunings to be skipped, e.g due to slow dbs
        val toleranceToAvoidTestFlakiness = 2

        pruningDoneCount should be > (secondsToRunTest - secondsWorstCaseUntilFirstPrune) / pruningInterval / toleranceToAvoidTestFlakiness
      }
  }
}
