// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.offboarding

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  StaticSynchronizerParameters,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.bftsequencer.AwaitsBftSequencerAuthenticationDisseminationQuorum
import com.digitalasset.canton.integration.util.OffboardsSequencerNode
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.SynchronizerId

class SequencerOffboardingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OffboardsSequencerNode
    with AwaitsBftSequencerAuthenticationDisseminationQuorum {

  // For full correctness we need to use >= 4 sequencers to test off-boarding because, when using the BFT sequencer,
  //  if the P2P gRPC channel crashes after the sequencer is off-boarded from Canton topology, but
  //  before it is removed from the ordering topology, its reconnection to the P2P network
  //  may fail due to failed authentication, rendering the BFT network stuck if there is
  //  no fault tolerance at all.
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S4M1_Manual

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

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
        sequencers = sequencers.local,
        // Bootstrapping the synchronizer with the mediator only connected to sequencer1
        //  because changing a mediator's connection is currently unsupported and
        //  the goal of this test is not to check sequencer connection fail-over.
        mediatorsToSequencers =
          Map(mediator1 -> (Seq(sequencer2), PositiveInt.one, NonNegativeInt.zero)),
        synchronizerOwners = synchronizerOwners,
        synchronizerThreshold = PositiveInt.two,
        staticParameters,
        mediatorRequestAmplification = SubmissionRequestAmplification.NoAmplification,
        mediatorThreshold = PositiveInt.one,
      )
    }
  }

  "Onboard participants to sequencers and send a ping" in { implicit env =>
    import env.*

    waitUntilAllBftSequencersAuthenticateDisseminationQuorum()

    clue("participant1 connects to sequencer1") {
      participant1.synchronizers.connect_local(sequencer1, daName)
    }
    clue("participant2 connects to sequencer2") {
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

  "Reconnect mediator2 to sequencer1" in { implicit env =>
    import env.*

    mediator1.sequencer_connection.set(SequencerConnections.single(sequencer1.sequencerConnection))
  }

  "Off-board sequencer2" in { implicit env =>
    import env.*

    offboardSequencer(
      synchronizerId,
      sequencerToOffboard = sequencer2,
      sequencersOnSynchronizer = NonEmpty(Seq, sequencer1),
      synchronizerOwners = synchronizerOwners.toSet,
      isBftOrderer = true,
    )
  }

  "Send a ping after off-boarding sequencer2" in { implicit env =>
    import env.*

    logger.info("Sending a ping after off-boarding sequencer2")

    participant2.health.ping(participant1.id)
  }
}
