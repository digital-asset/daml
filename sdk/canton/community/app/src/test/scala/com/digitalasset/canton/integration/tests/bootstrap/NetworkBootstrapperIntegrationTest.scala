// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bootstrap

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import scalaz.concurrent.Task.Try

class NetworkBootstrapperIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withManualStart

  registerPlugin(new UsePostgres(loggerFactory))

  "Network bootstrapper" should {

    /** Ensures that there is a helpful message when a previously executed network topology
      * description changes its synchronizer owners thereafter, and is executed again. This may
      * happen in an integration test as well as in a Canton script that uses a network bootstrapper
      * and is being run repeatedly (for example, for every node restart).
      */
    "raise meaningful error message" in { implicit env =>
      import env.*

      val network = NetworkTopologyDescription(
        daName,
        synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
      )

      val inconsistentNetwork = NetworkTopologyDescription(
        daName,
        synchronizerOwners = Seq[InstanceReference](sequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
      )

      // Should succeed
      new NetworkBootstrapper(network).bootstrap()

      // Should succeed (idempotency)
      new NetworkBootstrapper(network).bootstrap()

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        Try {
          new NetworkBootstrapper(inconsistentNetwork).bootstrap()
        }.toEither.left.value,
        LogEntry.assertLogSeq(
          Seq(
            (
              _.errorMessage should include(
                s"The synchronizer cannot be bootstrapped: ${sequencer1.id} has already been initialized for synchronizer $daId"
              ),
              "error",
            )
          )
        ),
      )

    }
  }
}
