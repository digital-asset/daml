// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.data.{CantonTimestamp, ReassignmentSubmitterMetadata}
import com.digitalasset.canton.ledger.participant.state.{
  CompletionInfo,
  Reassignment,
  ReassignmentInfo,
  SequencedUpdate,
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.conflictdetection.{ActivenessResult, CommitSet}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  FieldConversionError,
  ReassignmentProcessorError,
}
import com.digitalasset.canton.participant.protocol.validation.AuthenticationError
import com.digitalasset.canton.protocol.{
  DriverContractMetadata,
  LfContractId,
  LfNodeCreate,
  ReassignmentId,
  RootHash,
  SerializableContract,
}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}

import scala.concurrent.ExecutionContext

final case class AssignmentValidationResult(
    rootHash: RootHash,
    contract: SerializableContract,
    reassignmentCounter: ReassignmentCounter,
    submitterMetadata: ReassignmentSubmitterMetadata,
    reassignmentId: ReassignmentId,
    isReassigningParticipant: Boolean,
    hostedStakeholders: Set[LfPartyId],
    validationResult: AssignmentValidationResult.ValidationResult,
) extends ReassignmentValidationResult {

  override def isUnassignment: Boolean = false
  override def contractId: LfContractId = contract.contractId

  def isSuccessfulF(implicit ec: ExecutionContext): FutureUnlessShutdown[Boolean] =
    validationResult.isSuccessful

  def activenessResult: ActivenessResult = validationResult.activenessResult
  def authenticationErrorO: Option[AuthenticationError] = validationResult.authenticationErrorO
  def metadataResultET: EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] =
    validationResult.metadataResultET
  def validationErrors: Seq[ReassignmentValidationError] = validationResult.validationErrors

  private[reassignment] def commitSet = CommitSet(
    archivals = Map.empty,
    creations = Map.empty,
    unassignments = Map.empty,
    assignments = Map(
      contract.contractId ->
        CommitSet.AssignmentCommit(
          reassignmentId,
          contract.metadata,
          reassignmentCounter,
        )
    ),
  )

  private[reassignment] def createReassignmentAccepted(
      targetSynchronizer: Target[SynchronizerId],
      participantId: ParticipantId,
      targetProtocolVersion: Target[ProtocolVersion],
      recordTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, SequencedUpdate] = {

    val contractInst = contract.contractInstance.unversioned
    val createNode: LfNodeCreate =
      LfNodeCreate(
        coid = contract.contractId,
        templateId = contractInst.template,
        packageName = contractInst.packageName,
        arg = contractInst.arg,
        signatories = contract.metadata.signatories,
        stakeholders = contract.metadata.stakeholders,
        keyOpt = contract.metadata.maybeKeyWithMaintainers,
        version = contract.contractInstance.version,
      )
    val driverContractMetadata =
      DriverContractMetadata(contract.contractSalt).toLfBytes(targetProtocolVersion.unwrap)

    for {
      updateId <-
        rootHash.asLedgerTransactionId.leftMap[ReassignmentProcessorError](
          FieldConversionError(reassignmentId, "Transaction id (root hash)", _)
        )

      completionInfo =
        Option.when(participantId == submitterMetadata.submittingParticipant)(
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
      reassignment = Reassignment.Assign(
        ledgerEffectiveTime = contract.ledgerCreateTime.toLf,
        createNode = createNode,
        contractMetadata = driverContractMetadata,
      ),
      recordTime = recordTime,
    )
  }
}

object AssignmentValidationResult {
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

    def addValidationErrors(
        validationErrors: Seq[ReassignmentValidationError]
    ): ValidationResult =
      copy(validationErrors = validationErrors ++ this.validationErrors)

    def isUnassignmentDataNotFoundOrIncomplete: Boolean = validationErrors.exists {
      case AssignmentValidationError.UnassignmentDataNotFound(_) => true
      case AssignmentValidationError.UnassignmentIncomplete(_) => true
      case _ => false
    }
  }
}
