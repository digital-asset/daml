// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.entries

import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.ledger.api.domain.PackageEntry
import com.digitalasset.canton.platform.SubmissionId

private[platform] sealed abstract class PackageLedgerEntry extends Product with Serializable {
  def submissionId: SubmissionId

  def toDomain: PackageEntry
}

private[platform] object PackageLedgerEntry {

  final case class PackageUploadAccepted(
      submissionId: SubmissionId,
      recordTime: Timestamp,
  ) extends PackageLedgerEntry {
    override def toDomain: PackageEntry =
      PackageEntry.PackageUploadAccepted(
        submissionId,
        recordTime,
      )
  }

  final case class PackageUploadRejected(
      submissionId: SubmissionId,
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
