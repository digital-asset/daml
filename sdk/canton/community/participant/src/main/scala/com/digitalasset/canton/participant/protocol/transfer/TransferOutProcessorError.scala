// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.NonEmptyChain
import cats.implicits.catsSyntaxFoldableOps0
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.PackageUnknownTo
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.TransferProcessorError
import com.digitalasset.canton.participant.store.ActiveContractStore.Status
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult
import com.digitalasset.canton.protocol.{LfContractId, TransferId}
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.topology.DomainId

trait TransferOutProcessorError extends TransferProcessorError

object TransferOutProcessorError {

  def fromChain(constructor: String => TransferOutProcessorError)(
      errors: NonEmptyChain[String]
  ): TransferOutProcessorError =
    constructor(errors.mkString_(", "))

  final case class SubmittingPartyMustBeStakeholderOut(
      contractId: LfContractId,
      submittingParty: LfPartyId,
      stakeholders: Set[LfPartyId],
  ) extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out contract `$contractId`: submitter `$submittingParty` is not a stakeholder"
  }

  final case class UnexpectedDomain(transferId: TransferId, receivedOn: DomainId)
      extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out `$transferId`: received transfer on $receivedOn"
  }

  final case class TargetDomainIsSourceDomain(domain: DomainId, contractId: LfContractId)
      extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out contract `$contractId`: source and target domains are the same"
  }

  final case class UnknownContract(contractId: LfContractId) extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out contract `$contractId`: unknown contract"
  }

  final case class DeactivatedContract(contractId: LfContractId, status: Status)
      extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out contract `$contractId` because it's not active. Current status $status"
  }

  final case class InvalidResult(
      transferId: TransferId,
      result: DeliveredTransferOutResult.InvalidTransferOutResult,
  ) extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out `$transferId`: invalid result"
  }

  final case class AutomaticTransferInError(message: String) extends TransferOutProcessorError

  final case class PermissionErrors(message: String) extends TransferOutProcessorError

  final case class AdminPartyPermissionErrors(message: String) extends TransferOutProcessorError

  final case class StakeholderHostingErrors(message: String) extends TransferOutProcessorError

  final case class AdminPartiesMismatch(
      contractId: LfContractId,
      expected: Set[LfPartyId],
      declared: Set[LfPartyId],
  ) extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out contract `$contractId`: admin parties mismatch"
  }

  final case class RecipientsMismatch(
      contractId: LfContractId,
      expected: Option[Recipients],
      declared: Recipients,
  ) extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out contract `$contractId`: recipients mismatch"
  }

  final case class AbortedDueToShutdownOut(contractId: LfContractId)
      extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out contract `$contractId`: aborted due to shutdown"
  }

  final case class PackageIdUnknownOrUnvetted(
      contractId: LfContractId,
      unknownTo: List[PackageUnknownTo],
  ) extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer out contract `$contractId`: ${unknownTo.mkString(", ")}"
  }

}
