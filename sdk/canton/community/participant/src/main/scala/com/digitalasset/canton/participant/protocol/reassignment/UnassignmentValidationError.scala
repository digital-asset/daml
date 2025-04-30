// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.PackageUnknownTo
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.sequencing.protocol.Recipients

sealed trait UnassignmentValidationError extends ReassignmentValidationError

object UnassignmentValidationError {
  final case class PackageIdUnknownOrUnvetted(
      contractIds: Set[LfContractId],
      unknownTo: List[PackageUnknownTo],
  ) extends UnassignmentValidationError {
    override def message: String =
      s"Cannot unassign contracts `$contractIds`: ${unknownTo.mkString(", ")}"
  }

  final case class RecipientsMismatch(
      contractIds: Set[LfContractId],
      expected: Option[Recipients],
      declared: Recipients,
  ) extends UnassignmentValidationError {
    override def message: String =
      s"Cannot unassign contracts `$contractIds`: recipients mismatch"
  }
}
