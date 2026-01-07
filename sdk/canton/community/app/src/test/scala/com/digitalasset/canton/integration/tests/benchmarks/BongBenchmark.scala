// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.benchmarks

import com.digitalasset.canton.concurrent.ExecutionContextMonitor
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.ResourceLimits
import monocle.macros.syntax.lens.*
import org.scalatest.prop.TableFor2

import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait BongBenchmark extends CommunityIntegrationTest with SharedEnvironment with BongTestScenarios {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.warnIfOverloadedFor).replace(None) // no overloaded warnings
        ),
        _.focus(_.parameters.enableAdditionalConsistencyChecks)
          .replace(false), // because that would take way too long,
        // The default of 20 seconds is low and leads to flakiness
        ConfigTransforms.setDelayLoggingThreshold(NonNegativeFiniteDuration.ofSeconds(30)),
      )
      .withSetup { implicit env =>
        import env.*

        // Increase timeouts to avoid test flakiness
        runOnAllInitializedSynchronizersForAllOwners { (owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .propose_update(
              synchronizer.synchronizerId,
              _.update(
                confirmationResponseTimeout = 60.seconds,
                mediatorReactionTimeout = 60.seconds,
              ),
            )
        }

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)

        // Limits have been determined empirically.
        participants.all.foreach(
          _.resources.set_resource_limits(ResourceLimits(Some(40), Some(30)))
        )

        // Enable debug logging for the execution context monitor
        logging.set_level(
          s"${classOf[ExecutionContextMonitor].getName}:${this.getClass.getSimpleName}",
          "DEBUG",
        )
      }

  val levels: TableFor2[Int, FiniteDuration] = Table(
    ("level", "timeout"),
    (3, 1.minute),
    (6, 1.minute),
    (10, 5.minutes),
  )

  levels.forEvery { case (level, timeout) =>
    s"run the bong test at level $level" in { implicit env =>
      setupAndRunBongTest(level, timeout)(env)
    }
  }
}

class BongBenchmarkPostgres extends BongBenchmark {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
