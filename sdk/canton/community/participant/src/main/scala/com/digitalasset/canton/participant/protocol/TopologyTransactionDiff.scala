// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.*
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.topology.store.SignedTopologyTransactions.PositiveSignedTopologyTransactions
import com.digitalasset.canton.topology.transaction.*

// TODO(i21350): Handle changes to the domainId and authorization levels, also consider threshold
private[protocol] object TopologyTransactionDiff {

  private[protocol] def apply(
      old: PositiveSignedTopologyTransactions,
      current: PositiveSignedTopologyTransactions,
  ): Set[TopologyEvent] = {

    val before = partyToParticipant(old)
    val after = partyToParticipant(current)

    val added = after.diff(before).map { case (partyId, participantId) =>
      PartyToParticipantAuthorization(partyId, participantId, Submission)
    }
    val removed = before.diff(after).map { case (partyId, participantId) =>
      PartyToParticipantAuthorization(partyId, participantId, Revoked)
    }

    added ++ removed
  }

  private def partyToParticipant(state: PositiveSignedTopologyTransactions) =
    state
      .collectOfMapping[PartyToParticipant]
      .result
      .view
      .map(_.mapping)
      .flatMap(m => m.participants.map(p => (m.partyId.toLf, p.participantId.toLf)))
      .toSet
}
