// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.PackageUnknownTo
import com.digitalasset.canton.protocol.LfContractId

sealed trait UnassignmentValidationError extends ReassignmentValidationError

object UnassignmentValidationError {
  final case class PackageIdUnknownOrUnvetted(
      contractIds: Set[LfContractId],
      unknownTo: List[PackageUnknownTo],
  ) extends UnassignmentValidationError {
    override def message: String =
      s"Cannot unassign contracts `$contractIds`: ${unknownTo.mkString(", ")}"
  }

  case object TargetTimestampTooFarInFuture extends UnassignmentValidationError {
    override def message: String = "Target timestamp is too far in the future"
  }
}
