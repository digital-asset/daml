// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionId}
import com.digitalasset.ledger.api.domain.PartyDetails

//TODO BH consider removing duplication of this class
sealed abstract class PartyAllocationLedgerEntry() extends Product with Serializable {
  val submissionId: String
  val participantId: ParticipantId
  val recordTime: Instant
}

object PartyAllocationLedgerEntry {

  final case class Accepted(
      override val submissionId: SubmissionId,
      override val participantId: ParticipantId,
      override val recordTime: Instant,
      partyDetails: PartyDetails
  ) extends PartyAllocationLedgerEntry

  final case class Rejected(
      override val submissionId: SubmissionId,
      override val participantId: ParticipantId,
      override val recordTime: Instant,
      reason: String
  ) extends PartyAllocationLedgerEntry
}
