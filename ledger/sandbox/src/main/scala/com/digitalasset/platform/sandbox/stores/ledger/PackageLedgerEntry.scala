// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant
import com.daml.ledger.participant.state.v1.SubmissionId
import com.digitalasset.ledger.api.domain.PackageEntry

sealed abstract class PackageLedgerEntry extends Product with Serializable {
  def submissionId: SubmissionId
  def toDomain: PackageEntry
}

object PackageLedgerEntry {

  final case class PackageUploadAccepted(
      submissionId: SubmissionId,
      recordTime: Instant
  ) extends PackageLedgerEntry {
    override def toDomain: PackageEntry =
      PackageEntry.PackageUploadAccepted(
        submissionId,
        recordTime
      )
  }

  final case class PackageUploadRejected(
      submissionId: SubmissionId,
      recordTime: Instant,
      reason: String
  ) extends PackageLedgerEntry {
    override def toDomain: PackageEntry =
      PackageEntry.PackageUploadRejected(
        submissionId,
        recordTime,
        reason
      )
  }
}
