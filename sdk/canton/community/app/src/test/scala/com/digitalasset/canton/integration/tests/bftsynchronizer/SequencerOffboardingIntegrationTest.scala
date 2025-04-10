// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.OffboardsSequencerNode
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.{SequencerConnections, SubmissionRequestAmplification}
import com.digitalasset.canton.topology.SynchronizerId

trait SequencerOffboardingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OffboardsSequencerNode {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M1_Manual

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
        sequencers = Seq(sequencer1, sequencer2),
        // Bootstrapping the synchronizer with the mediator only connected to sequencer1
        //  because changing a mediator's connection is currently unsupported and
        //  the goal of this test is not to check sequencer connection fail-over.
        mediatorsToSequencers = Map(mediator1 -> (Seq(sequencer1), PositiveInt.one)),
        synchronizerOwners = synchronizerOwners,
        synchronizerThreshold = PositiveInt.two,
        staticParameters,
        mediatorRequestAmplification = SubmissionRequestAmplification.NoAmplification,
      )
    }
  }

  "Onboard participantX to sequencerX and send a ping" in { implicit env =>
    import env.*

    clue("participant1 connects to sequencer1") {
      participant1.synchronizers.connect_local(sequencer1, daName)
    }
    clue("participant2 connects to sequencer1") {
      participant2.synchronizers.connect_local(sequencer2, daName)
    }
    participant2.health.ping(participant1.id)
  }

  "Reconnect participant2 to sequencer1" in { implicit env =>
    import env.*

    participant2.synchronizers.disconnect(daName)
    participant2.synchronizers.modify(
      daName,
      _.copy(sequencerConnections = SequencerConnections.single(sequencer1.sequencerConnection)),
    )
    participant2.synchronizers.reconnect(daName)
  }

  "Off-board sequencer2" in { implicit env =>
    import env.*

    // Note that we're just off-boarding but not stopping `sequencer2` because, if the epoch is not over,
    //  it's still part of the ordering topology.
    //  Since we're using just 2 sequencers, there is no fault tolerance, which means consensus may rely
    //  on `sequencer2` and get stuck if it's switched off before it's also off-boarded from the ordering
    //  topology.
    offboardSequencer(
      synchronizerId,
      sequencerToOffboard = sequencer2,
      sequencerOnSynchronizer = sequencer1,
      synchronizerOwners = synchronizerOwners.toSet,
    )
  }

  "Send a ping after off-boarding sequencer2" in { implicit env =>
    import env.*

    logger.info("Sending a ping after off-boarding sequencer2")

    participant2.health.ping(participant1.id)
  }
}

class SequencerOffboardingReferenceIntegrationTestPostgres
    extends SequencerOffboardingIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

class SequencerOffboardingBftOrderingIntegrationTestPostgres
    extends SequencerOffboardingIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
