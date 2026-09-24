// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractsReassignmentBatch,
  ReassignmentSubmitterMetadata,
  UnassignmentData,
}
import com.digitalasset.canton.ledger.participant.state.{
  AcsChangeFactory,
  CompletionInfo,
  Reassignment,
  ReassignmentInfo,
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.ProcessingSteps.InternalContractIds
import com.digitalasset.canton.participant.protocol.conflictdetection.{ActivenessResult, CommitSet}
import com.digitalasset.canton.participant.protocol.validation.AuthenticationError
import com.digitalasset.canton.protocol.{ReassignmentId, RootHash, UpdateId}
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.canton.util.ReassignmentTag.Target

final case class UnassignmentValidationResult(
    unassignmentData: UnassignmentData,
    override val rootHash: RootHash,
    hostedConfirmingReassigningParties: Set[LfPartyId],
    // Defined iff the participant is reassigning
    assignmentExclusivity: Option[Target[CantonTimestamp]],
    commonValidationResult: UnassignmentValidationResult.CommonValidationResult,
    reassigningParticipantValidationResult: UnassignmentValidationResult.ReassigningParticipantValidationResult,
) extends ReassignmentValidationResult {
  val submitterMetadata: ReassignmentSubmitterMetadata = unassignmentData.submitterMetadata
  val sourceSynchronizer: ReassignmentTag.Source[PhysicalSynchronizerId] =
    unassignmentData.sourcePSId
  val targetSynchronizer: Target[PhysicalSynchronizerId] = unassignmentData.targetPSId
  val stakeholders: Set[LfPartyId] = unassignmentData.stakeholders.all
  val targetTimestamp: Target[CantonTimestamp] = unassignmentData.targetTimestamp

  override def reassignmentId: ReassignmentId = unassignmentData.reassignmentId

  def isReassigningParticipant: Boolean = assignmentExclusivity.isDefined

  override def activenessResultIsSuccessful: Boolean =
    commonValidationResult.activenessResult.isSuccessful

  def contracts: ContractsReassignmentBatch = unassignmentData.contractsBatch

  def commitSet: CommitSet = CommitSet(
    archivals = Map.empty,
    creations = Map.empty,
    assignments = Map.empty,
    unassignments = (contracts.contractIdCounters
      .map { case (contractId, reassignmentCounter) =>
        (
          contractId,
          CommitSet.UnassignmentCommit(
            targetSynchronizer.map(_.logical),
            stakeholders,
            reassignmentCounter,
          ),
        )
      })
      .toMap
      .forgetNE,
  )

  def createReassignmentAccepted(
      participantId: ParticipantId,
      recordTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): AcsChangeFactory => InternalContractIds => Update.SequencedReassignmentAccepted = {
    val updateId = rootHash
    val completionInfo =
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
    (acsChangeFactory: AcsChangeFactory) =>
      (internalContractIds: InternalContractIds) =>
        Update.SequencedReassignmentAccepted(
          optCompletionInfo = completionInfo,
          workflowId = submitterMetadata.workflowId,
          updateId = UpdateId.fromRootHash(updateId),
          reassignmentInfo = ReassignmentInfo(
            sourceSynchronizer = sourceSynchronizer.map(_.logical),
            targetSynchronizer = targetSynchronizer.map(_.logical),
            submitter = Option(submitterMetadata.submitter),
            reassignmentId = reassignmentId,
            isReassigningParticipant = isReassigningParticipant,
          ),
          reassignment =
            Reassignment.Batch(contracts.contracts.zipWithIndex.map { case (reassign, idx) =>
              Reassignment.Unassign(
                contractId = reassign.contract.contractId,
                templateId = reassign.templateId,
                packageName = reassign.packageName,
                stakeholders = contracts.stakeholders.all,
                assignmentExclusivity = assignmentExclusivity.map(_.unwrap.toLf),
                reassignmentCounter = reassign.counter.unwrap,
                nodeId = idx,
              )
            }),
          recordTime = recordTime,
          synchronizerId = sourceSynchronizer.unwrap.logical,
          acsChangeFactory = acsChangeFactory,
          internalContractIds = internalContractIds,
        )
  }
}

object UnassignmentValidationResult {
  final case class CommonValidationResult(
      activenessResult: ActivenessResult,
      participantSignatureVerificationResult: Option[AuthenticationError],
      contractAuthenticationResultF: EitherT[
        FutureUnlessShutdown,
        ReassignmentValidationError,
        Unit,
      ],
      submitterCheckResult: Option[ReassignmentValidationError],
  ) extends ReassignmentValidationResult.CommonValidationResult {

    // During unassignment the reassignment id is computed, rather than being passed in
    // so there's no need to validate it.
    def reassignmentIdResult: Option[ReassignmentValidationError] = None
  }

  final case class ReassigningParticipantValidationResult(
      errors: Seq[ReassignmentValidationError]
  ) extends ReassignmentValidationResult.ReassigningParticipantValidationResult {
    def isTargetTsValidatable: Boolean = !errors.exists {
      case UnassignmentValidationError.TargetTimestampTooFarInFuture => true
      case _ => false
    }
  }

  object ReassigningParticipantValidationResult {
    val TargetTimestampTooFarInFuture: ReassigningParticipantValidationResult =
      ReassigningParticipantValidationResult(
        Seq(UnassignmentValidationError.TargetTimestampTooFarInFuture)
      )
  }
}
