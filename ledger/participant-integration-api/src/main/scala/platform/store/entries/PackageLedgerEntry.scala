// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.entries

import com.daml.ledger.api.domain.PackageEntry
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp

private[platform] sealed abstract class PackageLedgerEntry extends Product with Serializable {
  def submissionId: Ref.SubmissionId

  def toDomain: PackageEntry
}

private[platform] object PackageLedgerEntry {

  final case class PackageUploadAccepted(
      submissionId: Ref.SubmissionId,
      recordTime: Timestamp,
  ) extends PackageLedgerEntry {
    override def toDomain: PackageEntry =
      PackageEntry.PackageUploadAccepted(
        submissionId,
        recordTime,
      )
  }

  final case class PackageUploadRejected(
      submissionId: Ref.SubmissionId,
      recordTime: Timestamp,
      reason: String,
  ) extends PackageLedgerEntry {
    override def toDomain: PackageEntry =
      PackageEntry.PackageUploadRejected(
        submissionId,
        recordTime,
        reason,
      )
  }
}
