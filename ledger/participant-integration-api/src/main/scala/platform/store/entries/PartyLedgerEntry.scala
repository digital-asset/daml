// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.entries

import java.time.Instant

import com.daml.ledger.api.domain.PartyDetails
import com.daml.lf.data.Ref

private[platform] sealed abstract class PartyLedgerEntry() extends Product with Serializable {
  val submissionIdOpt: Option[Ref.SubmissionId]
  val recordTime: Instant
}

private[platform] object PartyLedgerEntry {

  final case class AllocationAccepted(
      submissionIdOpt: Option[Ref.SubmissionId],
      recordTime: Instant,
      partyDetails: PartyDetails,
  ) extends PartyLedgerEntry

  final case class AllocationRejected(
      submissionId: Ref.SubmissionId,
      recordTime: Instant,
      reason: String,
  ) extends PartyLedgerEntry {
    override val submissionIdOpt: Option[Ref.SubmissionId] = Some(submissionId)
  }
}
