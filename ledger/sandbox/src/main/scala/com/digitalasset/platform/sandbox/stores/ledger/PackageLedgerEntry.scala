// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant
import com.daml.ledger.participant.state.v1.SubmissionId

sealed abstract class PackageLedgerEntry extends Product with Serializable {
  def submissionId: SubmissionId
}

object PackageLedgerEntry {

  final case class PackageUploadAccepted(
      submissionId: SubmissionId,
      recordTime: Instant
  ) extends PackageLedgerEntry

  final case class PackageUploadRejected(
      submissionId: SubmissionId,
      recordTime: Instant,
      reason: String
  ) extends PackageLedgerEntry
}
