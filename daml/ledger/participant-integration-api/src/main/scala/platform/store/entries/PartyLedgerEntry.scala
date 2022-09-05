// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.entries

import com.daml.ledger.api.domain.PartyDetails
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.SubmissionId

private[platform] sealed abstract class PartyLedgerEntry() extends Product with Serializable {
  val submissionIdOpt: Option[SubmissionId]
  val recordTime: Timestamp
}

private[platform] object PartyLedgerEntry {

  final case class AllocationAccepted(
      submissionIdOpt: Option[SubmissionId],
      recordTime: Timestamp,
      partyDetails: PartyDetails,
  ) extends PartyLedgerEntry

  final case class AllocationRejected(
      submissionId: SubmissionId,
      recordTime: Timestamp,
      reason: String,
  ) extends PartyLedgerEntry {
    override val submissionIdOpt: Option[SubmissionId] = Some(submissionId)
  }
}
