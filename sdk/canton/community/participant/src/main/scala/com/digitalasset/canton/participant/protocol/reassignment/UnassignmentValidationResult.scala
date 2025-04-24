// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree}
import com.digitalasset.canton.ledger.participant.state.{
  CompletionInfo,
  Reassignment,
  ReassignmentInfo,
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.conflictdetection.{ActivenessResult, CommitSet}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  FieldConversionError,
  ReassignmentProcessorError,
}
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationResult.ValidationResult
import com.digitalasset.canton.participant.protocol.validation.AuthenticationError
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

final case class UnassignmentValidationResult(
    // TODO(i25233): Right now we write the full tree as part of the unassignment data in the reassignment store.
    // we don't need all the tree to validate the assignment request
    fullTree: FullUnassignmentTree,
    reassignmentId: ReassignmentId,
    hostedStakeholders: Set[LfPartyId],
    assignmentExclusivity: Option[
      Target[CantonTimestamp]
    ], // Defined iff the participant is reassigning
    validationResult: ValidationResult,
) extends ReassignmentValidationResult {
  val rootHash = fullTree.rootHash
  val contractId = fullTree.contractId
  val reassignmentCounter = fullTree.reassignmentCounter
  val contract = fullTree.contract
  val templateId = contract.rawContractInstance.contractInstance.unversioned.template
  val packageName = contract.rawContractInstance.contractInstance.unversioned.packageName
  val submitterMetadata = fullTree.submitterMetadata
  val targetSynchronizer = fullTree.targetSynchronizer
  val stakeholders = fullTree.stakeholders.all
  val targetTimeProof = fullTree.targetTimeProof

  def isReassigningParticipant: Boolean = assignmentExclusivity.isDefined
  override def isUnassignment: Boolean = true
  def isSuccessfulF(implicit ec: ExecutionContext): FutureUnlessShutdown[Boolean] =
    validationResult.isSuccessful

  def activenessResult: ActivenessResult = validationResult.activenessResult
  def authenticationErrorO: Option[AuthenticationError] = validationResult.authenticationErrorO
  def metadataResultET: EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] =
    validationResult.metadataResultET
  def validationErrors: Seq[ReassignmentValidationError] = validationResult.validationErrors

  def commitSet = CommitSet(
    archivals = Map.empty,
    creations = Map.empty,
    unassignments = Map(
      contractId -> CommitSet
        .UnassignmentCommit(
          targetSynchronizer,
          stakeholders,
          reassignmentCounter,
        )
    ),
    assignments = Map.empty,
  )

  def createReassignmentAccepted(
      participantId: ParticipantId
  )(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, Update.SequencedReassignmentAccepted] =
    for {
      updateId <-
        rootHash.asLedgerTransactionId
          .leftMap[ReassignmentProcessorError](
            FieldConversionError(reassignmentId, "Transaction Id", _)
          )

      completionInfo =
        Option.when(
          participantId == submitterMetadata.submittingParticipant
        )(
          CompletionInfo(
            actAs = List(submitterMetadata.submitter),
            userId = submitterMetadata.userId,
            commandId = submitterMetadata.commandId,
            optDeduplicationPeriod = None,
            submissionId = submitterMetadata.submissionId,
          )
        )
    } yield Update.SequencedReassignmentAccepted(
      optCompletionInfo = completionInfo,
      workflowId = submitterMetadata.workflowId,
      updateId = updateId,
      reassignmentInfo = ReassignmentInfo(
        sourceSynchronizer = reassignmentId.sourceSynchronizer,
        targetSynchronizer = targetSynchronizer,
        submitter = Option(submitterMetadata.submitter),
        reassignmentCounter = reassignmentCounter.unwrap,
        unassignId = reassignmentId.unassignmentTs,
        isReassigningParticipant = isReassigningParticipant,
      ),
      reassignment = Reassignment.Unassign(
        contractId = contractId,
        templateId = templateId,
        packageName = packageName,
        stakeholders = stakeholders.toList,
        assignmentExclusivity = assignmentExclusivity.map(_.unwrap.toLf),
      ),
      recordTime = reassignmentId.unassignmentTs,
    )
}

object UnassignmentValidationResult {
  final case class ValidationResult(
      activenessResult: ActivenessResult,
      authenticationErrorO: Option[AuthenticationError],
      metadataResultET: EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit],
      validationErrors: Seq[ReassignmentValidationError],
  ) {
    def isSuccessful(implicit ec: ExecutionContext): FutureUnlessShutdown[Boolean] =
      for {
        modelConformanceCheck <- metadataResultET.value
      } yield activenessResult.isSuccessful && authenticationErrorO.isEmpty && validationErrors.isEmpty && modelConformanceCheck.isRight
  }
}
