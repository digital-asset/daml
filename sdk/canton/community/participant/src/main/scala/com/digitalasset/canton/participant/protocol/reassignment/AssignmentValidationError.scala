// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.daml.error.{Explanation, Resolution}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LocalRejectionGroup
import com.digitalasset.canton.error.{Alarm, AlarmErrorCode}
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}

sealed trait AssignmentValidationError extends ReassignmentValidationError

object AssignmentValidationError extends LocalRejectionGroup {
  final case class ReassignmentDataCompleted(reassignmentId: ReassignmentId)
      extends AssignmentValidationError {
    override def message: String =
      s"Cannot assign `$reassignmentId`: reassignment has been completed"
  }

  final case class ContractDataMismatch(reassignmentId: ReassignmentId)
      extends AssignmentValidationError {
    override def message: String = s"Cannot assign `$reassignmentId`: contract data mismatch"
  }

  @Explanation(
    """The participant has received an invalid delivered unassignment result.
      |This may occur due to a bug at the sender of the assignment."""
  )
  @Resolution("Contact support.")
  object InvalidUnassignmentResult
      extends AlarmErrorCode("PARTICIPANT_RECEIVED_INVALID_DELIVERED_UNASSIGNMENT_RESULT") {
    final case class DeliveredUnassignmentResultError(
        reassignmentId: ReassignmentId,
        error: String,
    ) extends Alarm(
          s"Cannot assign `$reassignmentId`: validation of DeliveredUnassignmentResult failed with error: $error"
        )
        with AssignmentValidationError {
      override def message: String = cause
    }
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

  final case class InconsistentReassignmentCounter(
      reassignmentId: ReassignmentId,
      declaredReassignmentCounter: ReassignmentCounter,
      expectedReassignmentCounter: ReassignmentCounter,
  ) extends AssignmentValidationError {
    override def message: String =
      s"Cannot assign $reassignmentId: reassignment counter $declaredReassignmentCounter in assignment does not match $expectedReassignmentCounter from the unassignment"
  }
}
