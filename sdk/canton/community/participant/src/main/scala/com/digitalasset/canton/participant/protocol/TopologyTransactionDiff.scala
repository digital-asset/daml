// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.*
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationLevel,
  TopologyEvent,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactions.PositiveSignedTopologyTransactions
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, *}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LedgerParticipantId, LedgerTransactionId, LfPartyId}

private[protocol] object TopologyTransactionDiff {

  /** Compute a set of topology events from the old state and the current state
    * @param synchronizerId
    *   synchronizer on which the topology transactions were sequenced
    * @param oldRelevantState
    *   Previous topology state
    * @param currentRelevantState
    *   Current state, after applying the batch of transactions
    * @param localParticipantId
    *   The local participant that may require initiation of online party replication
    * @return
    *   The set of events, the update_id, and whether a party needs to be replicated to this
    *   participant
    */
  private[protocol] def apply(
      synchronizerId: SynchronizerId,
      oldRelevantState: PositiveSignedTopologyTransactions,
      currentRelevantState: PositiveSignedTopologyTransactions,
      localParticipantId: ParticipantId,
      protocolVersion: ProtocolVersion,
  ): Option[TopologyTransactionDiff] = {

    val before = partyToParticipant(oldRelevantState)
    val after = partyToParticipant(currentRelevantState)

    def locallyAddedPartiesExistingOnOtherParticipants: Set[LfPartyId] = {
      // Adding a party that existed before on another participant means the local participant needs to
      // initiate party replication.
      val locallyAddedParties = after.view.collect {
        case ((partyId, participantId), _)
            if participantId == localParticipantId.toLf && !before.contains(
              partyId -> participantId
            ) =>
          partyId
      }.toSet
      val partiesExistingOnOtherParticipants = before.view.collect {
        case ((partyId, participantId), _) if participantId != localParticipantId.toLf => partyId
      }.toSet
      locallyAddedParties intersect partiesExistingOnOtherParticipants
    }

    val addedOrChanged: Set[TopologyEvent] = after.view.collect {
      case ((partyId, participantId), permission)
          if !before.get(partyId -> participantId).contains(permission) =>
        PartyToParticipantAuthorization(partyId, participantId, permission)
    }.toSet
    val removed: Set[TopologyEvent] = before.view.collect {
      case ((partyId, participantId), _) if !after.contains(partyId -> participantId) =>
        PartyToParticipantAuthorization(partyId, participantId, Revoked)
    }.toSet

    val allEvents: Set[TopologyEvent] = addedOrChanged ++ removed

    NonEmpty
      .from(allEvents)
      .map(
        TopologyTransactionDiff(
          _,
          updateId(synchronizerId, protocolVersion, oldRelevantState, currentRelevantState),
          requiresLocalParticipantPartyReplication =
            locallyAddedPartiesExistingOnOtherParticipants.nonEmpty,
        )
      )
  }

  private[protocol] def updateId(
      synchronizerId: SynchronizerId,
      protocolVersion: ProtocolVersion,
      oldRelevantState: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
      currentRelevantState: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ): LedgerTransactionId = {

    val builder = Hash.build(HashPurpose.TopologyUpdateId, HashAlgorithm.Sha256)
    def addToBuilder(
        stateTransactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]]
    ): Unit =
      stateTransactions
        .map(_.hashOfSignatures(protocolVersion).toHexString)
        .sorted // for not relying on retrieval order
        .foreach(builder.add)

    builder.add(synchronizerId.toProtoPrimitive)
    builder.add("old-relevant-state")
    addToBuilder(oldRelevantState)
    // the same state-tx can be either current or old, but these hashes should be different
    builder.add("new-relevant-state")
    addToBuilder(currentRelevantState)

    val hash = builder.finish()

    LedgerTransactionId.assertFromString(hash.toHexString)
  }

  private def partyToParticipant(
      state: PositiveSignedTopologyTransactions
  ): Map[(LfPartyId, LedgerParticipantId), AuthorizationLevel] = {
    val fromPartyToParticipantMapping = for {
      topologyTransaction <- SignedTopologyTransactions
        .collectOfMapping[TopologyChangeOp.Replace, PartyToParticipant](state)
        .view
      mapping = topologyTransaction.mapping
      participant <- mapping.participants
    } yield (
      mapping.partyId.toLf -> participant.participantId.toLf,
      toAuthorizationLevel(participant.permission),
    )
    val forAdminParties = SignedTopologyTransactions
      .collectOfMapping[TopologyChangeOp.Replace, SynchronizerTrustCertificate](state)
      .view
      .map(_.mapping)
      .map(m =>
        (
          m.participantId.adminParty.toLf -> m.participantId.toLf,
          AuthorizationLevel.Submission,
        )
      )
    fromPartyToParticipantMapping
      .++(forAdminParties)
      .toMap
  }

  private val toAuthorizationLevel: ParticipantPermission => AuthorizationLevel = {
    case ParticipantPermission.Submission => AuthorizationLevel.Submission
    case ParticipantPermission.Confirmation => AuthorizationLevel.Confirmation
    case ParticipantPermission.Observation => AuthorizationLevel.Observation
  }
}

private[protocol] final case class TopologyTransactionDiff(
    topologyEvents: NonEmpty[Set[TopologyEvent]],
    transactionId: LedgerTransactionId,
    requiresLocalParticipantPartyReplication: Boolean,
)
