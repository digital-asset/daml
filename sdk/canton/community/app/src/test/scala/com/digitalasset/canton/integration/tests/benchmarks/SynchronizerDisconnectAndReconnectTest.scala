// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.benchmarks

import com.digitalasset.canton.concurrent.ExecutionContextMonitor
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseConfigTransforms,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.ResourceLimits
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.DurationInt

sealed trait SynchronizerDisconnectAndReconnectTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BongTestScenarios {

  // This test is an adaptation of `BongBenchmark` meant to reproduce an issue that originated at a customer.
  // See https://github.com/DACH-NY/canton/issues/14150 for details.

  // The test tries hard to reproduce the conditions that trigger the bug, but due to timings and non-determinism,
  // there is no guarantee that it will fail at every execution if the bug is present.
  // However, if this test becomes flaky, there is a high probability that the bug, or something similar,
  // has been reintroduced in the code base.

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.warnIfOverloadedFor).replace(None) // no overloaded warnings
        ),
        _.focus(_.parameters.enableAdditionalConsistencyChecks)
          .replace(false), // because that would take way too long
      )
      .withSetup { implicit env =>
        import env.*

        // Increase timeouts to avoid test flakiness
        runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .propose_update(
              synchronizer.synchronizerId,
              _.update(
                confirmationResponseTimeout = 60.seconds,
                mediatorReactionTimeout = 60.seconds,
              ),
            )
        )

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)

        // Disconnect once from the synchronizer
        participants.all.synchronizers.disconnect(daName)
        // ... then reconnect
        participants.all.synchronizers.reconnect_all()

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

  val (level, timeout) = (10, 5.minutes)
  s"run the bong test at level $level" in { implicit env =>
    setupAndRunBongTest(level, timeout)(env)
  }
}

class SynchronizerDisconnectAndReconnectTestPostgres
    extends SynchronizerDisconnectAndReconnectTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(
    new UseConfigTransforms(
      Seq(
        // Enable replication to use the DbStorageMulti
        ConfigTransforms.enableReplicatedParticipants("participant1", "participant2"),
        // Limit the number of connections available to increase the likelihood of connection starving
        // and triggering a `NoActiveConnectionAvailable`
        ConfigTransforms.setParticipantMaxConnections(PositiveInt.one),
        // The default of 20 seconds is too low after switching to the AZ runners
        ConfigTransforms.setDelayLoggingThreshold(NonNegativeFiniteDuration.ofSeconds(30)),
      ),
      loggerFactory,
    )
  )
}
