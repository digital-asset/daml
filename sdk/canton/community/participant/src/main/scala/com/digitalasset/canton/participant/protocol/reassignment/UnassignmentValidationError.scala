// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.PackageUnknownTo
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.sequencing.protocol.Recipients

sealed trait UnassignmentValidationError extends ReassignmentValidationError

object UnassignmentValidationError {
  final case class PackageIdUnknownOrUnvetted(
      contractId: LfContractId,
      unknownTo: List[PackageUnknownTo],
  ) extends UnassignmentValidationError {
    override def message: String =
      s"Cannot unassign contract `$contractId`: ${unknownTo.mkString(", ")}"
  }

  final case class RecipientsMismatch(
      contractId: LfContractId,
      expected: Option[Recipients],
      declared: Recipients,
  ) extends UnassignmentValidationError {
    override def message: String =
      s"Cannot unassign contract `$contractId`: recipients mismatch"
  }
}
