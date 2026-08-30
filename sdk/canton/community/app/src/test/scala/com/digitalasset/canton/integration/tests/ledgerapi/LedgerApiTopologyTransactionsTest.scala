// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.daml.ledger.api.v2.state_service.ParticipantPermission
import com.daml.ledger.api.v2.state_service.ParticipantPermission.*
import com.daml.ledger.api.v2.topology_transaction.{
  ParticipantAuthorizationAdded,
  ParticipantAuthorizationRevoked,
  TopologyTransaction,
}
import com.digitalasset.canton.auth.AuthorizationChecksErrors.PermissionDenied
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, NonNegativeFiniteDuration}
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressingLogger, SuppressionRule}
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError.UnsafeToPrune
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceSynchronizerDisabledUs
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Remove
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission as PP,
  PartyToParticipant,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.{LfPartyId, config}
import org.scalatest.Assertion
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers.*
import org.slf4j.event.Level

import scala.annotation.nowarn

trait LedgerApiTopologyTransactionsTest extends CommunityIntegrationTest with SharedEnvironment {

  import LedgerApiTopologyTransactionsTest.*

  override val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)

  // Make sure deduplication duration does not block pruning
  private val maxDedupDuration = java.time.Duration.ofSeconds(2)
  private val reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)

  @nowarn("msg=match may not be exhaustive")
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Config
      .withNetworkBootstrap { implicit env =>
        val Seq(daDesc, acmeDesc) = EnvironmentDefinition.S1M1_S1M1
        new NetworkBootstrapper(
          daDesc,
          acmeDesc.withTopologyChangeDelay(NonNegativeFiniteDuration.ofSeconds(10)),
        )
      }
      .addConfigTransforms(
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration)
      )

  "topology transaction emitted for admin party after participant is connected to a synchronizer" in {
    implicit env =>
      import env.*

      val participants = List(participant1, participant2)
      val startOffsets = participants.map(_.ledger_api.state.end())

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant2.synchronizers.connect_local(sequencer1, alias = daName)

      participants.zip(startOffsets).foreach { case (participant, startOffset) =>
        withClue(participant) {
          val txs = participant.ledger_api.updates
            .topology_transactions(2, beginOffsetExclusive = startOffset)
          txs should have size 2
          txs
            .flatMap(_.topologyTransaction.events)
            .map(_.getParticipantAuthorizationAdded)
            .toSet shouldBe Set(
            ParticipantAuthorizationAdded(
              partyId = participant1.adminParty.toLf,
              participantId = participant1.id.toLf,
              participantPermission = ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
            ),
            ParticipantAuthorizationAdded(
              partyId = participant2.adminParty.toLf,
              participantId = participant2.id.toLf,
              participantPermission = ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
            ),
          )
        }
      }
  }

  "make sure that ACS commitments do not block pruning (preparation for later part in this test)" in {
    _.runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
      owner.topology.synchronizer_parameters
        .propose_update(
          synchronizer.synchronizerId,
          _.update(reconciliationInterval = reconciliationInterval),
        )
    )
  }

  "topology transaction emitted after party enabled on a participant" in { implicit env =>
    import env.*

    val participants = List(participant1, participant2)
    val startOffsets = List(participant1, participant2).map(_.ledger_api.state.end())

    val mahavishnu = participant1.parties.enable(
      "JohnMcLaughlin",
      synchronizeParticipants = participants,
    )
    val zappa = participant1.parties.enable(
      "FrankZappa",
      synchronizeParticipants = participants,
    )

    participants.zip(startOffsets).foreach { case (participant, startOffset) =>
      val txs = participant.ledger_api.updates
        .topology_transactions(1, Seq(zappa), startOffset)
        .map(_.topologyTransaction)
      val txsWild = participant.ledger_api.updates
        .topology_transactions(2, Nil, startOffset)
        .map(_.topologyTransaction)
      val partyToEffectiveTime = effectiveTimeMap(participant, daId)
      txsWild should have size 2
      txsWild.foreach { apiTopTx =>
        val party = LfPartyId.assertFromString(
          apiTopTx.events.headOption.value.event.participantAuthorizationAdded.value.partyId
        )
        val lapiRecordTime = CantonTimestamp.fromProtoTimestamp(apiTopTx.recordTime.value).value
        val topologyEffectiveTime = partyToEffectiveTime.get(party).value
        lapiRecordTime shouldBe topologyEffectiveTime
      }
      List(txs.headOption, txsWild.lastOption).foreach(
        assertAuthorizationAdded(
          zappa.toProtoPrimitive,
          participant1.adminParty.toProtoPrimitive,
          PARTICIPANT_PERMISSION_SUBMISSION,
        )(_)
      )
      assertAuthorizationAdded(
        mahavishnu.toProtoPrimitive,
        participant1.adminParty.toProtoPrimitive,
        PARTICIPANT_PERMISSION_SUBMISSION,
      )(txsWild.headOption)
    }
  }

  "topology transaction emitted after party changed its permission on a participant" in {
    implicit env =>
      import env.*

      val participants = List(participant1, participant2)

      val alice = participant1.parties.enable(
        "Alice" + "LedgerApiTopologyTransactionsTest",
        synchronizeParticipants = participants,
      )
      val bob = participant1.parties.enable(
        "Bob" + "LedgerApiTopologyTransactionsTest",
        synchronizeParticipants = participants,
      )

      val startOffsets = participants.map(_.ledger_api.state.end())

      participants.foreach(p =>
        p.topology.party_to_participant_mappings.propose_delta(
          alice,
          adds = participants.map(hostingParticipant => (hostingParticipant.id, Observation)),
          store = daId,
        )
      )

      participants.foreach(p =>
        p.topology.party_to_participant_mappings.propose_delta(
          bob,
          adds = participants.map(hostingParticipant => (hostingParticipant.id, Confirmation)),
          store = daId,
        )
      )

      participants.zip(startOffsets).foreach { case (participant, startOffset) =>
        val txs = participant.ledger_api.updates
          .topology_transactions(1, Seq(bob), startOffset)
          .map(_.topologyTransaction)
        val txsWild = participant.ledger_api.updates
          .topology_transactions(2, Nil, startOffset)
          .map(_.topologyTransaction)
        val partyToEffectiveTime = effectiveTimeMap(participant, daId)
        txsWild should have size 2
        txsWild.foreach { apiTopTx =>
          val party = LfPartyId.assertFromString(
            apiTopTx.events.flatMap(_.event.participantAuthorizationAdded).loneElement.partyId
          )
          val lapiRecordTime = CantonTimestamp.fromProtoTimestamp(apiTopTx.recordTime.value).value
          val topologyEffectiveTime = partyToEffectiveTime.get(party).value
          lapiRecordTime shouldBe topologyEffectiveTime
        }
        assertAuthorizationChanged(
          alice.toProtoPrimitive,
          participant1.uid.toProtoPrimitive,
          PARTICIPANT_PERMISSION_OBSERVATION,
        )(txsWild.headOption)
        assertAuthorizationAdded(
          alice.toProtoPrimitive,
          participant2.uid.toProtoPrimitive,
          PARTICIPANT_PERMISSION_OBSERVATION,
        )(txsWild.headOption)
        List(txs.headOption, txsWild.lastOption).foreach { tx =>
          assertAuthorizationChanged(
            bob.toProtoPrimitive,
            participant1.uid.toProtoPrimitive,
            PARTICIPANT_PERMISSION_CONFIRMATION,
          )(tx)
          assertAuthorizationAdded(
            bob.toProtoPrimitive,
            participant2.uid.toProtoPrimitive,
            PARTICIPANT_PERMISSION_CONFIRMATION,
          )(tx)
        }
      }

      // remove the participant2 again from the hosting for a later test case
      participant2.topology.party_to_participant_mappings.propose_delta(
        party = alice,
        removes = Seq(participant2.id),
        store = daId,
      )
      participant2.topology.party_to_participant_mappings.propose_delta(
        party = bob,
        removes = Seq(participant2.id),
        store = daId,
      )

  }

  "topology transaction emitted after party disabled on a participant" in { implicit env =>
    import env.*

    val participants = List(participant1, participant2)
    val startOffsets = List(participant1, participant2).map(_.ledger_api.state.end())

    val jon = participant1.parties.enable(
      "JonAnderson",
      synchronizeParticipants = participants,
    )
    val ian = participant1.parties.enable(
      "IanAnderson",
      synchronizeParticipants = participants,
    )

    val midOffsets = participants.zip(startOffsets).map { case (participant, startOffset) =>
      participant.ledger_api.updates
        .topology_transactions(2, Seq(), startOffset) // acts as synchronization
      participant.ledger_api.state.end()
    }

    participant1.parties.disable(jon)
    participant1.parties.disable(ian)

    participants.zip(midOffsets).foreach { case (participant, midOffset) =>
      val txs = participant.ledger_api.updates
        .topology_transactions(1, Seq(ian), midOffset)
        .map(_.topologyTransaction)
      val txsWild = participant.ledger_api.updates
        .topology_transactions(2, Seq(), midOffset)
        .map(_.topologyTransaction)
      val partyToEffectiveTime = effectiveTimeMap(participant, daId, operation = Some(Remove))
      txsWild.foreach { apiTopTx =>
        val party = LfPartyId.assertFromString(
          apiTopTx.events.headOption.value.event.participantAuthorizationRevoked.value.partyId
        )
        val lapiRecordTime = CantonTimestamp.fromProtoTimestamp(apiTopTx.recordTime.value).value
        val topologyEffectiveTime = partyToEffectiveTime.get(party).value
        lapiRecordTime shouldBe topologyEffectiveTime
      }
      List(txs.headOption, txsWild.lastOption).foreach(
        assertAuthorizationRevoked(
          ian.toProtoPrimitive,
          participant1.adminParty.toProtoPrimitive,
        )(_)
      )
      assertAuthorizationRevoked(
        jon.toProtoPrimitive,
        participant1.adminParty.toProtoPrimitive,
      )(txsWild.headOption)
    }

    val secondStartOffsets = List(participant1, participant2).map(_.ledger_api.state.end())

    val jonA = participant1.parties.enable(
      "JonAnderson",
      synchronizeParticipants = participants,
    )
    participant1.parties.disable(jonA)

    participants.zip(secondStartOffsets).map { case (participant, startOffset) =>
      // wait for both enable and disable to arrive at the Ledger API
      participant.ledger_api.updates
        .topology_transactions(2, Seq(), startOffset) should have size 2
    }
  // at this point the new update IDs are generated and persisted, so Ledger API DB verifcation should catch update ID duplication if any
  }

  "topology transaction emitted after party disabled completely" in { implicit env =>
    import env.*

    val participants = List(participant1)
    val startOffset = participant1.ledger_api.state.end()

    val bagheera = participant1.parties.enable(
      "Bagheera",
      synchronizeParticipants = participants,
    )

    val midOffset = {
      participant1.ledger_api.updates
        .topology_transactions(1, Seq(), startOffset) // acts as synchronization
      participant1.ledger_api.state.end()
    }

    participant1.parties.disable(bagheera)

    val txs = participant1.ledger_api.updates
      .topology_transactions(1, Seq(bagheera), midOffset)
      .map(_.topologyTransaction)
    val txsWild = participant1.ledger_api.updates
      .topology_transactions(1, Seq(), midOffset)
      .map(_.topologyTransaction)
    List(txs.headOption, txsWild.lastOption).foreach(
      assertAuthorizationRevoked(
        bagheera.toProtoPrimitive,
        participant1.adminParty.toProtoPrimitive,
      )(_)
    )
  }

  "topology transaction emitted after party replicated to another participant" in { implicit env =>
    import env.*

    val participants = List(participant1, participant2)
    val startOffset = participant1.ledger_api.state.end()

    val zawinul = participant1.parties.enable(
      "JoeZawinul",
      synchronizeParticipants = participants,
    )

    val midOffset = {
      participant1.ledger_api.updates
        .topology_transactions(1, Seq(), startOffset) // acts as synchronization
      participant1.ledger_api.state.end()
    }

    PartyToParticipantDeclarative.forParty(Set(participant1, participant2), daId)(
      participant1,
      zawinul,
      PositiveInt.one,
      Set(
        (participant1, PP.Submission),
        (participant2.id, PP.Submission),
      ),
    )

    val txs = participant1.ledger_api.updates
      .topology_transactions(1, Seq(zawinul), midOffset)
      .map(_.topologyTransaction)
    val txsWild = participant1.ledger_api.updates
      .topology_transactions(1, Seq(), midOffset)
      .map(_.topologyTransaction)
    List(txs.headOption, txsWild.lastOption).foreach(
      assertAuthorizationAdded(
        zawinul.toProtoPrimitive,
        participant2.adminParty.toProtoPrimitive,
      )(_)
    )

    // remove JoeZawinul hosting from participant2 for later test case
    participant2.topology.party_to_participant_mappings.propose_delta(
      zawinul,
      removes = List(participant2.id),
      store = daId,
    )
  }

  "topology transaction emitted after synchronizer is reconnected if it was not emitted before" in {
    implicit env =>
      import env.*

      // run this test case against a synchronizer with a large topology change delay and a fresh participant,
      // so that the other synchronizer used in this test doesn't interfere with its pruning timestamps
      eventually() {
        sequencer2.topology.sequencers
          .list(acmeId)
          .flatMap(_.item.active.forgetNE)
          .loneElement shouldBe sequencer2.id

      }
      participant3.synchronizers.connect_local(sequencer2, acmeName)
      participant3.health.ping(participant3, synchronizerId = acmeId)

      val babayaga = participant3.parties.enable(
        "Babayaga",
        synchronizeParticipants = Seq(),
        synchronize = None,
        synchronizer = acmeName,
      )
      val (sequencedTime, effectiveTime) = eventually() {
        participant3.topology.transactions
          .list(
            timeQuery = TimeQuery.Snapshot(wallClock.now.plusSeconds(20)),
            store = acmeId,
            filterMappings = List(TopologyMapping.Code.PartyToParticipant),
          )
          .result
          .flatMap(_.selectMapping[PartyToParticipant])
          .find(_.transaction.mapping.partyId == babayaga)
          .map(topologyTx =>
            (
              topologyTx.sequenced.value,
              topologyTx.validFrom.value,
            )
          )
          .value
      }
      // making sure that the topology transaction is fully processed
      eventually() {
        val synchronizerIndex = participant3.testing.state_inspection
          .lookupCleanSynchronizerIndex(acmeName)
          .value
          .failOnShutdown
          .futureValue
          .value
        logger.info(
          s"Sequenced time: $sequencedTime, current record time: ${synchronizerIndex.recordTime}, effective time: $effectiveTime"
        )
        synchronizerIndex.sequencerIndex.value.sequencerTimestamp should be >= sequencedTime
      }

      participant3.synchronizers.disconnect(acmeName)
      participant3.synchronizers.reconnect(acmeName, synchronize = None)

      // making sure that the topology update is not visible just yet: synchronizer time did not reach the effective time yet
      // (crash recovery for topology events should just schedule the event, not emit it)
      participant3.ledger_api.updates
        .topology_transactions(
          1,
          Seq(babayaga),
          0,
          Some(participant3.ledger_api.state.end()),
        ) should have size 0

      // making sure that the effective time is not yet passed after synchronizer reconnect
      val currentRecordTime = participant3.testing.state_inspection
        .lookupCleanSynchronizerIndex(acmeName)
        .value
        .failOnShutdown
        .futureValue
        .value
        .recordTime
      logger.info(
        s"Sequenced time: $sequencedTime, current record time: $currentRecordTime, effective time: $effectiveTime"
      )
      currentRecordTime should be >= sequencedTime
      currentRecordTime should be < effectiveTime

      participant3.health.ping(participant3, synchronizerId = acmeId)
      val ledgerEnd = participant3.ledger_api.state.end()

      // After a while the not-yet effective topology transaction should be the cause for pruning error
      eventually() {
        participant3.health.ping(participant3)
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant3.pruning.prune(ledgerEnd),
          logEntry => {
            logEntry.errorMessage should include(UnsafeToPrune.id)
            logEntry.errorMessage should include("due to Topology event crash recovery")
          },
        )
      }

      // the topology update is getting emitted on effective time
      eventually() {
        val updates = participant3.ledger_api.updates
          .topology_transactions(1, Seq(babayaga), 0, Some(participant3.ledger_api.state.end()))
          .map(_.topologyTransaction)
        updates should have size 1
        CantonTimestamp
          .fromProtoTimestamp(
            updates.headOption.value.recordTime.value
          )
          .value shouldBe effectiveTime
      }

      // and after effective time emission the participant can be pruned above the related sequenced time
      participant3.pruning.prune(ledgerEnd)

      participant3.synchronizers.disconnect(acmeName)
  }

  "Topology transaction is emitted in case SynchronizerTrustCertificate revocation" in {
    implicit env =>
      import env.*

      loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.WARN))(
        {
          val startOffset = participant1.ledger_api.state.end()

          // Revoke participant2's DTC on daId
          participant2.topology.synchronizer_trust_certificates.propose(
            participant2.id,
            daId,
            store = Some(daId),
            change = TopologyChangeOp.Remove,
            // as soon as the sequencer will see that the trust certificate is revoked, it will remove the participant and thus we cannot synchronize this topology change
            synchronize = None,
          )
          // disconnect from the synchronizer to avoid futile attempts to re-broadcast the revocation, in case the sequencer
          // terminates the connection before the participant receives the topology broadcast itself.
          participant2.synchronizers.disconnect(daName)

          eventually() {
            val txsWild = participant1.ledger_api.updates
              .topology_transactions(1, Seq(), startOffset)
              .map(_.topologyTransaction)
            txsWild should have size 1
            txsWild
              .flatMap(_.events)
              .map(_.getParticipantAuthorizationRevoked)
              .toSet shouldBe Set(
              ParticipantAuthorizationRevoked(
                partyId = participant2.adminParty.toLf,
                participantId = participant2.id.toLf,
              )
            )
          }
        },
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq.empty,
          mayContain = Seq(
            _.warningMessage should include("Token refresh aborted due to shutdown"),
            // Participant3 may attempt to connect to the sequencer (e.g., via a health check), but fail
            // because the connection to the sequencer has been "revoked".
            _.warningMessage should include("Request failed for sequencer."),
            // in case the MemberAuthenticationService terminates the connection
            _.shouldBeCantonErrorCode(PermissionDenied),
            // CantonSyncService disconnect
            _.shouldBeCantonError(
              SyncServiceSynchronizerDisabledUs,
              _ should include(s"$daName rejected our subscription"),
            ),
          ),
        ),
      )
  }
}

private object LedgerApiTopologyTransactionsTest {

  def assertAuthorizationAdded(
      expectedParty: String,
      expectedParticipant: String,
      expectedPermission: ParticipantPermission = PARTICIPANT_PERMISSION_SUBMISSION,
  )(actual: Option[TopologyTransaction]): Assertion = {
    val topologyEvent = actual
      .flatMap(_.events.headOption)
      .flatMap(_.event.participantAuthorizationAdded)
    inside(topologyEvent) { case Some(te) =>
      te.partyId shouldBe expectedParty
      te.participantId shouldBe expectedParticipant
      te.participantPermission shouldBe expectedPermission
    }
  }

  def assertAuthorizationChanged(
      expectedParty: String,
      expectedParticipant: String,
      expectedPermission: ParticipantPermission = PARTICIPANT_PERMISSION_SUBMISSION,
  )(actual: Option[TopologyTransaction]): Assertion = {
    val topologyEvent = actual.toList
      .flatMap(_.events)
      .flatMap(_.event.participantAuthorizationChanged)
      .headOption
    inside(topologyEvent) { case Some(te) =>
      te.partyId shouldBe expectedParty
      te.participantId shouldBe expectedParticipant
      te.participantPermission shouldBe expectedPermission
    }
  }

  def assertAuthorizationRevoked(
      expectedParty: String,
      expectedParticipant: String,
  )(actual: Option[TopologyTransaction]): Assertion = {
    val topologyEvent = actual
      .flatMap(_.events.headOption)
      .flatMap(_.event.participantAuthorizationRevoked)
    inside(topologyEvent) { case Some(te) =>
      te.partyId shouldBe expectedParty
      te.participantId shouldBe expectedParticipant
    }
  }

  def effectiveTimeMap(
      participant: LocalParticipantReference,
      synchronizerId: SynchronizerId,
      operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
  ): Map[LfPartyId, CantonTimestamp] =
    participant.topology.transactions
      .list(
        store = synchronizerId,
        operation = operation,
        filterMappings = List(TopologyMapping.Code.PartyToParticipant),
      )
      .result
      .flatMap(_.selectMapping[PartyToParticipant])
      .map(topologyTx =>
        (
          topologyTx.transaction.mapping.partyId.toLf,
          topologyTx.validFrom.value,
        )
      )
      .toMap
}

class LedgerApiTopologyTransactionsTestDefault extends LedgerApiTopologyTransactionsTest {
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.H2](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
}
