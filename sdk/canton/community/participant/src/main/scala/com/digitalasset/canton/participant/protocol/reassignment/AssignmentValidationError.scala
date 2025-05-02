// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LocalRejectionGroup
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}

sealed trait AssignmentValidationError extends ReassignmentValidationError

object AssignmentValidationError extends LocalRejectionGroup {
  final case class AssignmentCompleted(reassignmentId: ReassignmentId)
      extends AssignmentValidationError {
    override def message: String =
      s"Cannot assign `$reassignmentId`: assignment has been completed"
  }

  final case class ContractDataMismatch(reassignmentId: ReassignmentId)
      extends AssignmentValidationError {
    override def message: String = s"Cannot assign `$reassignmentId`: contract data mismatch"
  }

  final case class UnassignmentDataNotFound(reassignmentId: ReassignmentId)
      extends AssignmentValidationError {
    override def message: String = s"Cannot assign `$reassignmentId`: unassignment data not found"
  }

  final case class NonInitiatorSubmitsBeforeExclusivityTimeout(
      reassignmentId: ReassignmentId,
      submitter: LfPartyId,
      currentTimestamp: CantonTimestamp,
      timeout: Target[CantonTimestamp],
  ) extends AssignmentValidationError {
    override def message: String =
      s"Cannot assign `$reassignmentId`: only submitter can initiate before exclusivity timeout $timeout"
  }

  final case class InconsistentReassignmentCounters(
      reassignmentId: ReassignmentId,
      declaredReassignmentCounters: Map[LfContractId, ReassignmentCounter],
      expectedReassignmentCounters: Map[LfContractId, ReassignmentCounter],
  ) extends AssignmentValidationError {
    override def message: String =
      s"Cannot assign $reassignmentId: reassignment counters $declaredReassignmentCounters in assignment do not match $expectedReassignmentCounters from the unassignment"
  }
}
