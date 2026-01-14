// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{DbConfig, DefaultProcessingTimeouts}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.resource.{DbStorage, StorageSingleFactory}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.time.SimClock

abstract class DownloadTopologyForInitIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq(sequencer1, sequencer2),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1, sequencer2),
            mediators = Seq(mediator1),
            overrideMediatorToSequencers = Some(
              Map(
                // A threshold of 2 ensures that the mediators connect to all sequencers.
                // TODO(#19911) Make this properly configurable
                mediator1 -> (Seq(sequencer1, sequencer2), PositiveInt.two, NonNegativeInt.zero)
              )
            ),
          )
        )
      }

  "detect a hash disagreement among sequencers" in { implicit env =>
    import env.*

    // we make sequencer1 malicious by deleting the topology mappings for mediator1 from the db
    val factory = new StorageSingleFactory(sequencer1.config.storage)
    val storage = factory.tryCreate(
      connectionPoolForParticipant = false,
      None,
      new SimClock(CantonTimestamp.Epoch, loggerFactory),
      None,
      CommonMockMetrics.dbStorage,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    ) match {
      case jdbc: DbStorage => jdbc
      case _ => fail("should be db storage")
    }
    import storage.api.*
    storage
      .update(
        sqlu"""delete from common_topology_transactions where identifier = ${mediator1.id.member.identifier};""",
        "test-delete-mediator-topology-mapping",
      )
      .futureValueUS should be > 0
    storage.close()

    val sequencerClient =
      mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator.sequencerClient

    // Need to restart the sequencer due to potentially cached initial topology state hash
    // that will not pick up the removed db record above
    sequencer1.stop()
    sequencer1.start()
    sequencer1.health.wait_for_running()

    val errorMessage = sequencerClient
      .downloadTopologyStateForInit(0, retryLogLevel = None)
      .futureValueUS
      .leftOrFail("Expected a hash disagreement")

    errorMessage should include("FailedToReachThreshold")
  }

}

class DownloadTopologyForInitIntegrationTestPostgres
    extends DownloadTopologyForInitIntegrationTest {

  registerPlugin(new UsePostgres(loggerFactory))
  // TODO(#29833): Graceful shutdown of BftBlockOrderer
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
