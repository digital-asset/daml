// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.config
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, TopologyChangeOp}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

/** Test that the system still works properly after parties are left dangling
  *
  * A dangling party is a party for which one of the participants has become defunct. What we test
  * here is the following scenario:
  *   - Alice on P1 and P2
  *   - Both have a contract
  *   - P2 sends an OTK remove
  *   - Alice still can exercise on their contracts via P1
  */
class DanglingPartiesIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils {

  private var alice: PartyId = _
  private var storeId: TopologyStoreId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransform(ConfigTransforms.updateAllParticipantConfigs { case (_, config) =>
        config.focus(_.topology.disableOptionalTopologyChecks).replace(true)
      })
      .addConfigTransform(
        _.focus(_.monitoring.logging.eventDetails)
          .replace(true)
          .focus(_.monitoring.logging.api.maxMessageLines)
          .replace(10000)
          .focus(_.monitoring.logging.api.maxStringLength)
          .replace(40000)
      )
      .withSetup { env =>
        import env.*

        // make sure we send enough commitments
        Seq[InstanceReference](mediator1, sequencer1).foreach(
          _.topology.synchronizer_parameters
            .propose_update(
              sequencer1.synchronizer_id,
              _.update(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
            )
            .discard
        )

        participants.all.foreach { p =>
          p.synchronizers.connect_local(sequencer1, daName)
          p.dars.upload(CantonExamplesPath)
        }

        storeId = TopologyStoreId.Synchronizer(sequencer1.synchronizer_id)
        alice = PartyId.tryCreate("Alice", participant1.id.namespace)
        participant1.topology.party_to_participant_mappings.propose(
          alice,
          newParticipants = Seq(
            (participant1.id, ParticipantPermission.Submission),
            (participant2.id, ParticipantPermission.Confirmation),
          ),
          store = storeId,
        )

        Seq((participant2, alice)).foreach { case (participant, party) =>
          val proposal = eventually() {
            participant.topology.party_to_participant_mappings
              .list_hosting_proposals(sequencer1.synchronizer_id, participant.id)
              .loneElement
          }
          participant.topology.transactions.authorize(sequencer1.synchronizer_id, proposal.txHash)
          eventually() {
            participant.ledger_api.parties.list().map(_.party) should contain(party)
          }
        }
      }

  "create contracts for alice and bob" in { implicit env =>
    import env.*
    createCycleContract(participant1, alice, id = "CYCLE1")
  }

  "unregister p2 by removing otk" in { implicit env =>
    import env.*

    val otk = participant2.topology.owner_to_key_mappings
      .list(store = Some(storeId), filterKeyOwnerUid = participant2.filterString)
      .loneElement
    participant2.topology.transactions.propose(
      otk.item,
      storeId,
      change = TopologyChangeOp.Remove,
    )
    eventually() {
      val all = participant1.topology.owner_to_key_mappings
        .list(
          store = Some(storeId),
          filterKeyOwnerUid = participant2.filterString,
          timeQuery = TimeQuery.Range(None, None),
          operation = None,
        )
      all.lastOption
        .valueOrFail("Must exist")
        .context
        .operation shouldBe TopologyChangeOp.Remove
    }
    // TODO(#28232) automatically disconnect from synchronizer
    participant2.synchronizers.disconnect_all()

  }

  "contract with p2 is still alive" in { implicit env =>
    import env.*

    // TODO(#28232) without the eventually, p2 will be addressed because p1 still thinks that p2 is active
    //   once we make it impossible to remove OTK that are still in use, we can switch this test to STC
    eventually() {
      participant1.topology.owner_to_key_mappings.list(
        sequencer1.synchronizer_id,
        filterKeyOwnerUid = participant2.filterString,
      ) shouldBe empty
    }

    loggerFactory.assertLogsSeq(
      SuppressionRule
        .LevelAndAbove(Level.WARN)
    )(
      awaitAndExerciseCycleContract(participant1, alice),
      messages =>
        forAll(messages) { msg =>
          msg.warningMessage should include(
            "has a synchronizer trust certificate, but no keys on synchronizer"
          )
        },
    )
  }

}
