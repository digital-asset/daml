// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.ledger.api.domain.PartyDetails

//TODO BH consider removing duplication of this class
sealed abstract class PartyAllocationLedgerEntry() extends Product with Serializable {
  val submissionId: String
  val participantId: ParticipantId
  def value: String
}

object PartyAllocationLedgerEntry {

  final case class Accepted(
      override val submissionId: String,
      override val participantId: ParticipantId,
      partyDetails: PartyDetails
  ) extends PartyAllocationLedgerEntry {
    override def value = "accept"
  }

  final case class Rejected(
      override val submissionId: String,
      override val participantId: ParticipantId,
      reason: String
  ) extends PartyAllocationLedgerEntry {
    override def value = "reject"
  }
}
