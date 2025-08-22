// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractsReassignmentBatch,
  ReassignmentSubmitterMetadata,
}
import com.digitalasset.canton.ledger.participant.state.{
  AcsChangeFactory,
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
import com.digitalasset.canton.protocol.{LfNodeCreate, ReassignmentId, RootHash}
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import AssignmentValidationResult.*

final case class AssignmentValidationResult private[reassignment] (
    rootHash: RootHash,
    contracts: ContractsReassignmentBatch,
    submitterMetadata: ReassignmentSubmitterMetadata,
    reassignmentId: ReassignmentId,
    sourcePSId: Source[PhysicalSynchronizerId],
    hostedConfirmingReassigningParties: Set[LfPartyId],
    isReassigningParticipant: Boolean,
    commonValidationResult: CommonValidationResult,
    reassigningParticipantValidationResult: ReassigningParticipantValidationResult,
) extends ReassignmentValidationResult {

  override def activenessResultIsSuccessful: Boolean = {

    // The activeness check is performed at request time and may flag the reassignmentId as inactive
    // if the unassignment is still in progress. Once the unassignment is complete, its data becomes
    // available in the reassignment cache. If the activeness check flags the reassignmentId as inactive
    // but the reassignment cache indicates it is known and the assignment is not yet completed,
    // the activeness check can be considered valid.
    val isReassignmentActive: Boolean =
      !reassigningParticipantValidationResult.isUnassignmentDataNotFound &&
        !reassigningParticipantValidationResult.isAssignmentCompleted &&
        commonValidationResult.activenessResult.inactiveReassignments.contains(reassignmentId)
        && commonValidationResult.activenessResult.contracts.isSuccessful

    commonValidationResult.activenessResult.isSuccessful || isReassignmentActive
  }

  private[reassignment] def commitSet = CommitSet.createForAssignment(
    reassignmentId,
    contracts.contracts,
    sourcePSId.map(_.logical),
  )

  private[reassignment] def createReassignmentAccepted(
      targetSynchronizer: Target[SynchronizerId],
      participantId: ParticipantId,
      recordTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, AcsChangeFactory => SequencedUpdate] = {
    val reassignment = contracts.contracts.zipWithIndex.map { case (reassign, idx) =>
      val contract = reassign.contract
      val contractInst = contract.inst
      val createNode = LfNodeCreate(
        coid = contract.contractId,
        templateId = contractInst.templateId,
        packageName = contractInst.packageName,
        arg = contractInst.createArg,
        signatories = contract.metadata.signatories,
        stakeholders = contract.metadata.stakeholders,
        keyOpt = contract.metadata.maybeKeyWithMaintainers,
        version = contractInst.version,
      )
      Reassignment.Assign(
        ledgerEffectiveTime = contract.inst.createdAt.time,
        createNode = createNode,
        contractAuthenticationData = contract.inst.authenticationData,
        reassignmentCounter = reassign.counter.unwrap,
        nodeId = idx,
      )
    }
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
    } yield (acsChangeFactory: AcsChangeFactory) =>
      Update.SequencedReassignmentAccepted(
        optCompletionInfo = completionInfo,
        workflowId = submitterMetadata.workflowId,
        updateId = updateId,
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = sourcePSId.map(_.logical),
          targetSynchronizer = targetSynchronizer,
          submitter = Option(submitterMetadata.submitter),
          reassignmentId = reassignmentId,
          isReassigningParticipant = isReassigningParticipant,
        ),
        reassignment = Reassignment.Batch(reassignment),
        recordTime = recordTime,
        synchronizerId = targetSynchronizer.unwrap,
        acsChangeFactory = acsChangeFactory,
      )
  }
}

object AssignmentValidationResult {
  final case class CommonValidationResult(
      activenessResult: ActivenessResult,
      participantSignatureVerificationResult: Option[AuthenticationError],
      contractAuthenticationResultF: EitherT[
        FutureUnlessShutdown,
        ReassignmentValidationError,
        Unit,
      ],
      submitterCheckResult: Option[ReassignmentValidationError],
      reassignmentIdResult: Option[ReassignmentValidationError],
  ) extends ReassignmentValidationResult.CommonValidationResult

  final case class ReassigningParticipantValidationResult(
      errors: Seq[ReassignmentValidationError]
  ) extends ReassignmentValidationResult.ReassigningParticipantValidationResult {

    def isUnassignmentDataNotFound: Boolean = errors.exists {
      case AssignmentValidationError.UnassignmentDataNotFound(_) => true
      case _ => false
    }

    def isAssignmentCompleted: Boolean = errors.exists {
      case AssignmentValidationError.AssignmentCompleted(_) => true
      case _ => false
    }
  }
}
