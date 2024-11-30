// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.*
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactions.PositiveSignedTopologyTransactions
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LedgerParticipantId, LedgerTransactionId, LfPartyId}

// TODO(i21350): Handle changes to the domainId and authorization levels, also consider threshold
private[protocol] object TopologyTransactionDiff {

  /** Compute a set of topology events from the old state and the current state
    * @param domainId Domain on which the topology transactions were sequenced
    * @param old Previous topology state
    * @param current Current state, after applying the batch of transactions
    * @param transactions The batch of transactions that lead to the current state
    * @return The set of events and the update_id
    */
  private[protocol] def apply(
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      old: PositiveSignedTopologyTransactions,
      current: PositiveSignedTopologyTransactions,
      transactions: Seq[GenericSignedTopologyTransaction],
  ): Option[(NonEmpty[Set[TopologyEvent]], LedgerTransactionId)] = {

    val before = partyToParticipant(old)
    val after = partyToParticipant(current)

    val added: Set[TopologyEvent] = after.diff(before).map { case (partyId, participantId) =>
      PartyToParticipantAuthorization(partyId, participantId, Submission)
    }
    val removed: Set[TopologyEvent] = before.diff(after).map { case (partyId, participantId) =>
      PartyToParticipantAuthorization(partyId, participantId, Revoked)
    }

    val allEvents: Set[TopologyEvent] = added ++ removed

    NonEmpty.from(allEvents).map((_, updateId(domainId, protocolVersion, transactions)))
  }

  private[protocol] def updateId(
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      txs: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ): LedgerTransactionId = {

    val builder = Hash.build(HashPurpose.TopologyUpdateId, HashAlgorithm.Sha256)
    builder.add(domainId.toProtoPrimitive)
    txs.foreach(tx => builder.add(tx.hashOfSignatures(protocolVersion).getCryptographicEvidence))

    val hash = builder.finish()

    LedgerTransactionId.assertFromString(hash.toHexString)
  }

  private def partyToParticipant(
      state: PositiveSignedTopologyTransactions
  ): Set[(LfPartyId, LedgerParticipantId)] =
    SignedTopologyTransactions
      .collectOfMapping[TopologyChangeOp.Replace, PartyToParticipant](state)
      .view
      .map(_.mapping)
      .flatMap(m => m.participants.map(p => (m.partyId.toLf, p.participantId.toLf)))
      .toSet
}
