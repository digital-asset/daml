// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
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
import com.digitalasset.canton.participant.protocol.validation.AuthenticationError
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target

final case class UnassignmentValidationResult(
    // TODO(i25233): Right now we write the full tree as part of the unassignment data in the reassignment store.
    // we don't need all the tree to validate the assignment request
    fullTree: FullUnassignmentTree,
    reassignmentId: ReassignmentId,
    hostedStakeholders: Set[LfPartyId],
    assignmentExclusivity: Option[
      Target[CantonTimestamp]
    ], // Defined iff the participant is reassigning
    unassignmentTs: CantonTimestamp,
    commonValidationResult: UnassignmentValidationResult.CommonValidationResult,
    reassigningParticipantValidationResult: UnassignmentValidationResult.ReassigningParticipantValidationResult,
) extends ReassignmentValidationResult {
  val rootHash = fullTree.rootHash
  val submitterMetadata = fullTree.submitterMetadata
  val sourceSynchronizer = fullTree.sourceSynchronizer
  val targetSynchronizer = fullTree.targetSynchronizer
  val stakeholders = fullTree.stakeholders.all
  val targetTimeProof = fullTree.targetTimeProof

  def isReassigningParticipant: Boolean = assignmentExclusivity.isDefined

  override def activenessResultIsSuccessful: Boolean =
    commonValidationResult.activenessResult.isSuccessful

  def contracts = fullTree.contracts

  def commitSet = CommitSet(
    archivals = Map.empty,
    creations = Map.empty,
    assignments = Map.empty,
    unassignments = (contracts.contractIdCounters
      .map { case (contractId, reassignmentCounter) =>
        (
          contractId,
          CommitSet.UnassignmentCommit(targetSynchronizer, stakeholders, reassignmentCounter),
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
    )
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
  ) extends ReassignmentValidationResult.CommonValidationResult

  final case class ReassigningParticipantValidationResult(
      errors: Seq[ReassignmentValidationError]
  ) extends ReassignmentValidationResult.ReassigningParticipantValidationResult
}
