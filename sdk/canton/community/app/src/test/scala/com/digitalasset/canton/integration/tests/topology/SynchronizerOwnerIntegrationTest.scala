// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.tag.Security.SecurityTestSuite
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.TopologyChangeOp

trait SynchronizerOwnerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with SecurityTestSuite
    with AccessTestScenario {

  private var alice: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap { implicit env =>
        import env.*
        val owners = sequencers.all ++ mediators.all
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = owners,
            synchronizerThreshold = PositiveInt.tryCreate(owners.size),
            sequencers = sequencers.all,
            mediators = mediators.all,
          )
        )
      }
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(CantonExamplesPath)
        alice = participant1.parties.enable("alice")
      }

  "synchronizer owners can remove a fully authorized topology mapping not controlled by the synchronizer namespace" in {
    implicit env =>
      import env.*

      createCycleContract(participant1, alice, "dangling")

      checkCycleContractExists()

      val aliceReplace = synchronizerOwners1
        .map { owner =>
          eventually() {
            val alicePTP = owner.topology.party_to_participant_mappings
              .list(daId, filterParty = alice.filterString)
              .loneElement
            alicePTP.context.serial shouldBe PositiveInt.one
            alicePTP.context.operation shouldBe TopologyChangeOp.Replace
            alicePTP
          }
        }
        .headOption
        .value
      synchronizerOwners1.foreach { owner =>
        owner.topology.party_to_participant_mappings.propose(
          party = aliceReplace.item.partyId,
          newParticipants =
            aliceReplace.item.participants.map(hp => (hp.participantId, hp.permission)),
          threshold = aliceReplace.item.threshold,
          serial = Some(aliceReplace.context.serial.increment),
          operation = TopologyChangeOp.Remove,
          store = daId,
        )
      }

      // check that participant1 doesn't see alice anymore as registered party on the synchronizer
      eventually() {
        val aliceOnSynchronizers =
          participant1.parties.list(alice.filterString).map(_.participants.flatMap(_.synchronizers))
        aliceOnSynchronizers shouldBe empty

        val ptps =
          participant1.topology.party_to_participant_mappings.list(daId).map(_.item.partyId).toSet
        ptps should not contain alice

        val aliceRemove = participant1.topology.party_to_participant_mappings
          .list(
            daId,
            filterParty = alice.filterString,
            operation = Some(TopologyChangeOp.Remove),
          )
          .loneElement

        aliceRemove.item shouldBe aliceReplace.item
        aliceRemove.context.operation shouldBe TopologyChangeOp.Remove
        aliceRemove.context.serial shouldBe aliceReplace.context.serial.increment
      }

      // the cycle contract should still exist, although the participant can't really do anything with it
      checkCycleContractExists()
  }

  private def checkCycleContractExists()(implicit env: TestConsoleEnvironment) =
    env.participant1.ledger_api.javaapi.state.acs.await(M.Cycle.COMPANION)(alice)

}

//class SynchronizerOwnerReferenceIntegrationTestPostgres extends SynchronizerOwnerIntegrationTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
//}

class SynchronizerOwnerBftOrderingIntegrationTestPostgres extends SynchronizerOwnerIntegrationTest {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
