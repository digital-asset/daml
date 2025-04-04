// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.OnboardsNewSequencerNode
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.synchronizer.sequencer.store.DbSequencerStore
import com.digitalasset.canton.topology.SynchronizerId
import org.slf4j.event.Level

trait SequencerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OnboardsNewSequencerNode {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Manual

  private var synchronizerId: SynchronizerId = _
  private var staticParameters: StaticSynchronizerParameters = _
  private var synchronizerOwners: Seq[InstanceReference] = _

  "Basic synchronizer startup" in { implicit env =>
    import env.*

    clue("starting up participants") {
      // for now we need a participant to effect changes to the synchronizer after the initial bootstrap
      participant1.start()
      participant2.start()
    }
    clue("start sequencers") {
      sequencers.local.start()
    }
    clue("start mediator") {
      mediator1.start()
    }

    staticParameters =
      StaticSynchronizerParameters.defaults(sequencer1.config.crypto, testedProtocolVersion)

    synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1)
    synchronizerId = clue("bootstrapping the synchronizer") {
      bootstrap.synchronizer(
        "test-synchronizer",
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
        synchronizerOwners = synchronizerOwners,
        synchronizerThreshold = PositiveInt.two,
        staticParameters,
      )
    }
  }

  "Generate some traffic" in { implicit env =>
    import env.*

    clue("participant1 connects to sequencer1") {
      participant1.synchronizers.connect_local(sequencer1, daName)
    }
    clue("participant2 connects to sequencer1") {
      participant2.synchronizers.connect_local(sequencer1, daName)
    }
    // ping
    participant2.health.ping(participant1.id)

    // enable a party
    participant1.parties.enable(
      "alice",
      synchronizeParticipants = Seq(participant1, participant2),
    )

    // ping again
    participant1.health.ping(participant2.id)
  }

  "Onboard a new sequencer" in { implicit env =>
    import env.*
    onboardNewSequencer(
      synchronizerId,
      newSequencer = sequencer2,
      existingSequencer = sequencer1,
      synchronizerOwners = synchronizerOwners.toSet,
    )
  }

  "Onboard participant2 to sequencer2 and send a ping" in { implicit env =>
    import env.*

    clue("participant1 connects to sequencer1") {
      participant1.synchronizers.connect_local(sequencer1, daName)
    }
    clue("participant2 connects to sequencer2") {
      participant2.synchronizers.disconnect_local(daName)
      participant2.synchronizers.connect_local(sequencer2, daName)
    }
    participant2.health.ping(participant1.id)
  }

  "preload events into the sequencer cache" in { implicit env =>
    import env.*
    loggerFactory.assertLogsSeq(
      SuppressionRule.Level(Level.INFO) && SuppressionRule.forLogger[DbSequencerStore]
    )(
      {
        sequencer1.stop()
        sequencer1.start()
      },
      entries => {
        forAtLeast(1, entries) {
          _.infoMessage should (include regex s"Preloading the events buffer with a memory limit of .*")
        }
        forAtLeast(1, entries) {
          _.infoMessage should (include regex ("Loaded [1-9]\\d* events between .*? and .*?".r))
        }
      },
    )

  }

}

// TODO(#18401): Re-enable the following tests when SequencerStore creation has been moved to the factory
//class SequencerIntegrationTestDefault extends SequencerIntegrationTest {
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//}

class SequencerIntegrationTestPostgres extends SequencerIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
