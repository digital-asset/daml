// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Inspection.{
  SynchronizerTimeRange,
  TimeRange,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.console.{CommandFailure, InstanceReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.ReceivedCmtState
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.TopologyManagerError.InvalidOwnerToKeyMappingRemoval
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  TopologyChangeOp,
}
import monocle.macros.syntax.lens.*

/** Test that the system still works properly after parties are left dangling
  *
  * A dangling party is a party for which one of the participants has become defunct. What we test
  * here is the following scenario:
  *   - Alice on P1 and P2
  *   - Both have a contract
  *   - P2 sends an STC remove (OTK remove would fail)
  *   - Alice still can exercise on their contracts via P1
  */
class DanglingPartiesIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils {

  private var alice: PartyId = _
  private var storeId: TopologyStoreId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransform(ConfigTransforms.updateAllParticipantConfigs { case (_, config) =>
        config.focus(_.topology.disableOptionalTopologyChecks).replace(true)
      })
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
            (participant3.id, ParticipantPermission.Confirmation),
          ),
          store = storeId,
        )

        Seq(participant2, participant3).foreach { participant =>
          val proposal = eventually() {
            participant.topology.party_to_participant_mappings
              .list_hosting_proposals(sequencer1.synchronizer_id, participant.id)
              .loneElement
          }
          participant.topology.transactions.authorize(sequencer1.synchronizer_id, proposal.txHash)
        }

        participants.all.foreach { p =>
          eventually() {
            val mapping =
              p.topology.party_to_participant_mappings.list(sequencer1.synchronizer_id).loneElement
            mapping.item.partyId shouldBe alice
            mapping.item.participants.sortBy(_.participantId) should matchPattern {
              case Seq(
                    HostingParticipant(
                      p1,
                      ParticipantPermission.Submission,
                      false, // onboarding
                    ),
                    HostingParticipant(
                      p2,
                      ParticipantPermission.Confirmation,
                      false, // onboarding
                    ),
                    HostingParticipant(
                      p3,
                      ParticipantPermission.Confirmation,
                      false, // onboarding
                    ),
                  ) if (participant1.id == p1 && participant2.id == p2 && participant3.id == p3) =>
            }
          }
        }
      }

  "create contracts for alice and bob" in { implicit env =>
    import env.*
    createCycleContract(participant1, alice, id = "CYCLE1")
  }

  "unregister p2 by removing stc" in { implicit env =>
    import env.*

    val otk = participant2.topology.owner_to_key_mappings
      .list(store = Some(storeId), filterKeyOwnerUid = participant2.filterString)
      .loneElement
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant2.topology.transactions.propose(
        otk.item,
        storeId,
        change = TopologyChangeOp.Remove,
      ),
      _.shouldBeCantonErrorCode(InvalidOwnerToKeyMappingRemoval),
    )
    val stc = participant2.topology.synchronizer_trust_certificates
      .list(store = Some(storeId), filterUid = participant2.filterString)
      .loneElement
    def awaitRemoval(query: TimeQuery) =
      eventually() {
        participant1.topology.synchronizer_trust_certificates
          .list(
            store = Some(storeId),
            filterUid = participant2.id.filterString,
            timeQuery = query,
            operation = Some(TopologyChangeOp.Remove),
          ) should not be empty
      }
    // depending on whoever observes the STC removal first, this might get pretty noisy
    loggerFactory.suppressWarningsAndErrors {
      participant2.topology.transactions.propose(
        stc.item,
        storeId,
        change = TopologyChangeOp.Remove,
      )
      // wait until tx is observed
      awaitRemoval(TimeQuery.Range(None, None))
      // p2 will be automatically booted but we disconnect here to avoid warnings
      participant2.synchronizers.disconnect_all()
    }

    // wait until tx has become effective
    awaitRemoval(TimeQuery.HeadState)

  }

  "contract with p2 is still alive" in { implicit env =>
    import env.*

    awaitAndTouchCycleContract(participant1, alice)
  }

  "commitments are still being updated" in { implicit env =>
    import env.*

    val t1 = env.environment.clock.now

    eventually() {
      val received = participant1.commitments.lookup_received_acs_commitments(
        synchronizerTimeRanges = Seq(
          SynchronizerTimeRange(
            sequencer1.synchronizer_id,
            timeRange = Some(
              TimeRange(
                t1,
                t1.plusMillis(1500),
              )
            ),
          )
        ),
        counterParticipants = Seq(participant3.id),
        commitmentState = Seq.empty,
        verboseMode = true,
      ) getOrElse (sequencer1.synchronizer_id, Seq.empty)

      if (received.exists(_.state == ReceivedCmtState.Match)) succeed
      else {
        // trigger a time advancement
        awaitAndTouchCycleContract(participant1, alice)
        fail("commitment is still outstanding")
      }
    }

  }

}
