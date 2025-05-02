// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractsReassignmentBatch,
  ReassignmentSubmitterMetadata,
}
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
  CantonContractIdVersion,
  DriverContractMetadata,
  LfNodeCreate,
  ReassignmentId,
  RootHash,
}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

final case class AssignmentValidationResult(
    rootHash: RootHash,
    contracts: ContractsReassignmentBatch,
    submitterMetadata: ReassignmentSubmitterMetadata,
    reassignmentId: ReassignmentId,
    isReassigningParticipant: Boolean,
    hostedStakeholders: Set[LfPartyId],
    validationResult: AssignmentValidationResult.ValidationResult,
) extends ReassignmentValidationResult {

  override def isUnassignment: Boolean = false

  override def activenessResultIsSuccessful: Boolean =
    validationResult.activenessResultIsSuccessful(reassignmentId)

  def isSuccessfulF(implicit ec: ExecutionContext): FutureUnlessShutdown[Boolean] =
    validationResult.isSuccessful(reassignmentId)

  def activenessResult: ActivenessResult = validationResult.activenessResult
  def authenticationErrorO: Option[AuthenticationError] = validationResult.authenticationErrorO
  def metadataResultET: EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] =
    validationResult.metadataResultET
  def validationErrors: Seq[ReassignmentValidationError] = validationResult.validationErrors

  private[reassignment] def commitSet = CommitSet(
    archivals = Map.empty,
    creations = Map.empty,
    unassignments = Map.empty,
    assignments = contracts.contracts
      .map { case reassign =>
        reassign.contract.contractId -> CommitSet.AssignmentCommit(
          reassignmentId,
          reassign.contract.metadata,
          reassign.counter,
        )
      }
      .toMap
      .forgetNE,
  )

  private[reassignment] def createReassignmentAccepted(
      targetSynchronizer: Target[SynchronizerId],
      participantId: ParticipantId,
      recordTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, SequencedUpdate] = {
    val reassignment =
      Reassignment.Batch(contracts.contracts.zipWithIndex.map { case (reassign, idx) =>
        val contract = reassign.contract
        val contractInst = contract.contractInstance.unversioned
        val createNode = LfNodeCreate(
          coid = contract.contractId,
          templateId = contractInst.template,
          packageName = contractInst.packageName,
          arg = contractInst.arg,
          signatories = contract.metadata.signatories,
          stakeholders = contract.metadata.stakeholders,
          keyOpt = contract.metadata.maybeKeyWithMaintainers,
          version = contract.contractInstance.version,
        )
        val contractIdVersion =
          CantonContractIdVersion.tryCantonContractIdVersion(contract.contractId)
        val driverContractMetadata =
          DriverContractMetadata(contract.contractSalt).toLfBytes(contractIdVersion)

        Reassignment.Assign(
          ledgerEffectiveTime = contract.ledgerCreateTime.toLf,
          createNode = createNode,
          contractMetadata = driverContractMetadata,
          reassignmentCounter = reassign.counter.unwrap,
          nodeId = idx,
        )
      })

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
        unassignId = reassignmentId.unassignmentTs,
        isReassigningParticipant = isReassigningParticipant,
      ),
      reassignment = reassignment,
      recordTime = recordTime,
      synchronizerId = targetSynchronizer.unwrap,
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
    def isSuccessful(
        reassignmentId: ReassignmentId
    )(implicit ec: ExecutionContext): FutureUnlessShutdown[Boolean] =
      for {
        modelConformanceCheck <- metadataResultET.value
      } yield activenessResultIsSuccessful(
        reassignmentId
      ) && authenticationErrorO.isEmpty && validationErrors.isEmpty && modelConformanceCheck.isRight

    private[reassignment] def addValidationErrors(
        validationErrors: Seq[ReassignmentValidationError]
    ): ValidationResult =
      copy(validationErrors = validationErrors ++ this.validationErrors)

    def isUnassignmentDataNotFound: Boolean = validationErrors.exists {
      case AssignmentValidationError.UnassignmentDataNotFound(_) => true
      case _ => false
    }

    private def isAssignmentCompleted: Boolean = validationErrors.exists {
      case AssignmentValidationError.AssignmentCompleted(_) => true
      case _ => false
    }

    private[reassignment] def activenessResultIsSuccessful(
        reassignmentId: ReassignmentId
    ): Boolean = {

      // The activeness check is performed at request time and may flag the reassignmentId as inactive
      // if the unassignment is still in progress. Once the unassignment is complete, its data becomes
      // available in the reassignment cache. If the activeness check flags the reassignmentId as inactive
      // but the reassignment cache indicates it is known and the assignment is not yet completed,
      // the activeness check can be considered valid.
      val isReassignmentActive: Boolean =
        !isUnassignmentDataNotFound && !isAssignmentCompleted && activenessResult.inactiveReassignments
          .contains(reassignmentId)
          && activenessResult.contracts.isSuccessful

      activenessResult.isSuccessful || isReassignmentActive
    }
  }
}
