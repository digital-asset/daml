// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.ReassignmentRef
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ReassignmentProcessorError,
  SubmissionValidationError,
}
import com.digitalasset.canton.protocol.{ContractMetadata, LfContractId, Stakeholders}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.daml.lf.engine

trait ReassignmentValidationError extends Serializable with Product with PrettyPrinting {
  override protected def pretty: Pretty[ReassignmentValidationError.this.type] =
    prettyOfString(_.message)
  def message: String

  def toSubmissionValidationError: ReassignmentProcessorError =
    SubmissionValidationError(s"Submission failed because: $message")
}

object ReassignmentValidationError {

  final case class ReinterpretationAborted(reassignmentRef: ReassignmentRef, reason: String)
      extends ReassignmentValidationError {
    override def message: String =
      s"For `$reassignmentRef`: reinterpretation aborted for reason `$reason`"
  }

  final case class ReinterpretationError(reassignmentRef: ReassignmentRef, reason: String)
      extends ReassignmentValidationError {
    override def message: String =
      s"For `$reassignmentRef`: reinterpretation failed for reason `$reason`"
  }

  final case class StakeholdersMismatch(
      reassignmentRef: ReassignmentRef,
      declaredViewStakeholders: Stakeholders,
      expectedStakeholders: Stakeholders,
  ) extends ReassignmentValidationError {
    override def message: String =
      s"For `$reassignmentRef`: stakeholders mismatch. " +
        s"Expected $expectedStakeholders, found $declaredViewStakeholders"
  }

  final case class ContractMetadataMismatch(
      reassignmentRef: ReassignmentRef,
      declaredContractMetadata: ContractMetadata,
      expectedMetadata: ContractMetadata,
  ) extends ReassignmentValidationError {
    override def message: String = s"For `$reassignmentRef`: metadata mismatch. " +
      s"Expected $expectedMetadata, found $declaredContractMetadata"
  }

  final case class NotHostedOnParticipant(
      reference: ReassignmentRef,
      party: LfPartyId,
      participantId: ParticipantId,
  ) extends ReassignmentValidationError {

    override def message: String =
      s"For $reference: $party is not hosted on $participantId"
  }

  final case class SubmitterMustBeStakeholder(
      reference: ReassignmentRef,
      submittingParty: LfPartyId,
      stakeholders: Set[LfPartyId],
  ) extends ReassignmentValidationError {
    override def message: String =
      s"For $reference: submitter `$submittingParty` is not a stakeholder"
  }

  final case class MetadataNotFound(err: engine.Error) extends ReassignmentValidationError {
    override def message: String = s"Contract metadata not found: ${err.message}"
  }

  final case class StakeholderHostingErrors(message: String) extends ReassignmentValidationError

  object StakeholderHostingErrors {
    def stakeholderNotHostedOnSynchronizer(
        missingStakeholders: Set[LfPartyId],
        synchronizer: ReassignmentTag[?],
    ): StakeholderHostingErrors =
      StakeholderHostingErrors(
        s"The following stakeholders are not active on the ${synchronizer.kind} synchronizer: $missingStakeholders"
      )

    def stakeholdersNoReassigningParticipant(
        stakeholders: Set[LfPartyId]
    ): StakeholderHostingErrors =
      StakeholderHostingErrors(
        s"The following stakeholders are not hosted on any reassigning participants: $stakeholders"
      )

    def missingSignatoryReassigningParticipants(
        signatory: LfPartyId,
        synchronizer: String,
        threshold: PositiveInt,
        signatoryReassigningParticipants: Int,
    ): StakeholderHostingErrors = StakeholderHostingErrors(
      s"Signatory $signatory requires at least $threshold signatory reassigning participants on synchronizer $synchronizer, but only $signatoryReassigningParticipants are available"
    )
  }

  final case class ReassigningParticipantsMismatch(
      reassignmentRef: ReassignmentRef,
      expected: Set[ParticipantId],
      declared: Set[ParticipantId],
  ) extends ReassignmentValidationError {
    override def message: String =
      s"For `$reassignmentRef`: reassigning participants mismatch"
  }

  final case class AbortedDueToShutdownOut(contractId: LfContractId)
      extends ReassignmentValidationError {
    override def message: String =
      s"Cannot unassign contract `$contractId`: aborted due to shutdown"
  }
}
