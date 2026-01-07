// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.functor.*
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
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.InternalContractIds
import com.digitalasset.canton.participant.protocol.conflictdetection.{ActivenessResult, CommitSet}
import com.digitalasset.canton.participant.protocol.validation.AuthenticationError
import com.digitalasset.canton.protocol.{LfNodeCreate, ReassignmentId, RootHash, UpdateId}
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{LfPartyId, checked}

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
    loggerFactory: NamedLoggerFactory,
) extends ReassignmentValidationResult
    with NamedLogging {

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

  // Assigning the internal contract ids to the contracts requires that all the contracts are
  // already persisted in the contract store.
  private[reassignment] def createReassignmentAccepted(
      targetSynchronizer: Target[SynchronizerId],
      participantId: ParticipantId,
      recordTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): AcsChangeFactory => InternalContractIds => SequencedUpdate = {
    val updateId = rootHash
    val completionInfo =
      Option.when(participantId == submitterMetadata.submittingParticipant)(
        CompletionInfo(
          actAs = List(submitterMetadata.submitter),
          userId = submitterMetadata.userId,
          commandId = submitterMetadata.commandId,
          optDeduplicationPeriod = None,
          submissionId = submitterMetadata.submissionId,
        )
      )
    (acsChangeFactory: AcsChangeFactory) =>
      (internalContractIds: InternalContractIds) =>
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
            internalContractId = checked {
              // the internal contract id must exist since we persisted all the contracts before
              internalContractIds.getOrElse(
                contract.contractId,
                ErrorUtil.invalidState(
                  s"The internal contract id for the assigned contract ${contract.contractId} was not found"
                ),
              )
            },
          )
        }
        Update.SequencedReassignmentAccepted(
          optCompletionInfo = completionInfo,
          workflowId = submitterMetadata.workflowId,
          updateId = UpdateId.fromRootHash(updateId),
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
