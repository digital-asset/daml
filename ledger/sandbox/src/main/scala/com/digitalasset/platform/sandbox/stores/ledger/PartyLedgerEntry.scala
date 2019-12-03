// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionId}
import com.digitalasset.ledger.api.domain.PartyDetails

sealed abstract class PartyLedgerEntry() extends Product with Serializable {
  val submissionId: String
  val recordTime: Instant
}

object PartyLedgerEntry {

  final case class AllocationAccepted(
      override val submissionId: SubmissionId,
      participantId: ParticipantId,
      override val recordTime: Instant,
      partyDetails: PartyDetails
  ) extends PartyLedgerEntry

  final case class AllocationRejected(
      override val submissionId: SubmissionId,
      participantId: ParticipantId,
      override val recordTime: Instant,
      reason: String
  ) extends PartyLedgerEntry

  final case class ImplicitPartyCreated(
      override val submissionId: SubmissionId,
      override val recordTime: Instant,
      partyDetails: PartyDetails
  ) extends PartyLedgerEntry
}
