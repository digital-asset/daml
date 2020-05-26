// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.entries

import java.time.Instant

import com.daml.ledger.participant.state.v1.SubmissionId
import com.daml.ledger.api.domain.PackageEntry

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
