// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.PackageUploadEntry

trait PackageConversion {
  def packageUploadLedgerEntryToDomain(
      ledgerEntry: PackageUploadLedgerEntry): domain.PackageUploadEntry =
    ledgerEntry match {
      case PackageUploadLedgerEntry.Accepted(submissionId, participantId, _) =>
        PackageUploadEntry.Accepted(submissionId, domain.ParticipantId(participantId))
      case PackageUploadLedgerEntry.Rejected(submissionId, participantId, _, reason) =>
        PackageUploadEntry.Rejected(submissionId, domain.ParticipantId(participantId), reason)
    }
}

object PackageConversion extends PackageConversion
