// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.PartyAllocationEntry

trait PartyConversion {
  def partyAllocationLedgerEntryToDomain(
      ledgerEntry: PartyAllocationLedgerEntry): domain.PartyAllocationEntry =
    ledgerEntry match {
      case PartyAllocationLedgerEntry.Accepted(submissionId, participantId, _, partyDetails) =>
        PartyAllocationEntry.Accepted(
          submissionId,
          domain.ParticipantId(participantId),
          partyDetails)
      case PartyAllocationLedgerEntry.Rejected(submissionId, participantId, _, reason) =>
        PartyAllocationEntry.Rejected(submissionId, domain.ParticipantId(participantId), reason)
    }
}

object PartyConversion extends PartyConversion
