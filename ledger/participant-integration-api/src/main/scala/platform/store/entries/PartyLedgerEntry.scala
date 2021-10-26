// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.entries

import com.daml.ledger.api.domain.PartyDetails
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp

private[platform] sealed abstract class PartyLedgerEntry() extends Product with Serializable {
  val submissionIdOpt: Option[Ref.SubmissionId]
  val recordTime: Timestamp
}

private[platform] object PartyLedgerEntry {

  final case class AllocationAccepted(
      submissionIdOpt: Option[Ref.SubmissionId],
      recordTime: Timestamp,
      partyDetails: PartyDetails,
  ) extends PartyLedgerEntry

  final case class AllocationRejected(
      submissionId: Ref.SubmissionId,
      recordTime: Timestamp,
      reason: String,
  ) extends PartyLedgerEntry {
    override val submissionIdOpt: Option[Ref.SubmissionId] = Some(submissionId)
  }
}
