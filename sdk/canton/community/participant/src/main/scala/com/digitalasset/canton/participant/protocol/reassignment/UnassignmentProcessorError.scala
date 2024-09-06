// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.NonEmptyChain
import cats.implicits.catsSyntaxFoldableOps0
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.PackageUnknownTo
import com.digitalasset.canton.participant.store.ActiveContractStore.Status
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.topology.DomainId

trait UnassignmentProcessorError extends ReassignmentProcessorError

object UnassignmentProcessorError {

  def fromChain(constructor: String => UnassignmentProcessorError)(
      errors: NonEmptyChain[String]
  ): UnassignmentProcessorError =
    constructor(errors.mkString_(", "))

  final case class SubmittingPartyMustBeStakeholderOut(
      contractId: LfContractId,
      submittingParty: LfPartyId,
      stakeholders: Set[LfPartyId],
  ) extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign contract `$contractId`: submitter `$submittingParty` is not a stakeholder"
  }

  final case class UnexpectedDomain(reassignmentId: ReassignmentId, receivedOn: DomainId)
      extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign `$reassignmentId`: received transfer on $receivedOn"
  }

  final case class TargetDomainIsSourceDomain(domain: DomainId, contractId: LfContractId)
      extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign contract `$contractId`: source and target domains are the same"
  }

  final case class UnknownContract(contractId: LfContractId) extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign contract `$contractId`: unknown contract"
  }

  final case class DeactivatedContract(contractId: LfContractId, status: Status)
      extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign contract `$contractId` because it's not active. Current status $status"
  }

  final case object ReassignmentCounterOverflow extends ReassignmentProcessorError {
    override def message: String = "Reassignment counter overflow"
  }
  final case class InvalidResult(
      reassignmentId: ReassignmentId,
      result: DeliveredUnassignmentResult.InvalidUnassignmentResult,
  ) extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign `$reassignmentId`: invalid result"
  }

  final case class AutomaticAssignmentError(message: String) extends UnassignmentProcessorError

  final case class PermissionErrors(message: String) extends UnassignmentProcessorError

  final case class AdminPartyPermissionErrors(message: String) extends UnassignmentProcessorError

  final case class StakeholderHostingErrors(message: String) extends UnassignmentProcessorError

  final case class AdminPartiesMismatch(
      contractId: LfContractId,
      expected: Set[LfPartyId],
      declared: Set[LfPartyId],
  ) extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign contract `$contractId`: admin parties mismatch"
  }

  final case class RecipientsMismatch(
      contractId: LfContractId,
      expected: Option[Recipients],
      declared: Recipients,
  ) extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign contract `$contractId`: recipients mismatch"
  }

  final case class AbortedDueToShutdownOut(contractId: LfContractId)
      extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot unassign contract `$contractId`: aborted due to shutdown"
  }

  final case class PackageIdUnknownOrUnvetted(
      contractId: LfContractId,
      unknownTo: List[PackageUnknownTo],
  ) extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot transfer out contract `$contractId`: ${unknownTo.mkString(", ")}"
  }

}
