// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.PartyEntry

trait PartyConversion {
  def partyAllocationLedgerEntryToDomain(ledgerEntry: PartyLedgerEntry): domain.PartyEntry =
    ledgerEntry match {
      case PartyLedgerEntry.AllocationAccepted(submissionId, participantId, _, partyDetails) =>
        PartyEntry.AllocationAccepted(
          submissionId,
          domain.ParticipantId(participantId),
          partyDetails)
      case PartyLedgerEntry.AllocationRejected(submissionId, participantId, _, reason) =>
        PartyEntry.AllocationRejected(submissionId, domain.ParticipantId(participantId), reason)
      case PartyLedgerEntry.ImplicitPartyCreated(submissionId, _, partyDetails) =>
        PartyEntry.ImplicitPartyCreated(submissionId, partyDetails)
    }
}

object PartyConversion extends PartyConversion
