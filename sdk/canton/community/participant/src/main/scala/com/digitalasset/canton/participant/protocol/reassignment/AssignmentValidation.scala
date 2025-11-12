// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.{EitherT, Validated}
import cats.implicits.toFunctorOps
import cats.syntax.foldable.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidationError.{
  AssignmentCompleted,
  ContractDataMismatch,
  InconsistentReassignmentCounters,
  NonInitiatorSubmitsBeforeExclusivityTimeout,
  UnassignmentDataNotFound,
  UnassignmentTimestampMismatch,
}
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidationResult.ReassigningParticipantValidationResult
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.validation.AuthenticationValidator
import com.digitalasset.canton.participant.protocol.{ProcessingSteps, reassignment}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ReassignmentStore.{
  AssignmentStartingBeforeUnassignment,
  ReassignmentCompleted,
  UnknownReassignmentId,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

private[reassignment] class AssignmentValidation(
    targetPSId: Target[PhysicalSynchronizerId],
    staticSynchronizerParameters: Target[StaticSynchronizerParameters],
    participantId: ParticipantId,
    reassignmentCoordination: ReassignmentCoordination,
    contractValidator: ContractValidator,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  /** Validate the assignment request
    */
  def perform(
      unassignmentDataE: Either[ReassignmentStore.ReassignmentLookupError, UnassignmentData],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  )(parsedRequest: ParsedReassignmentRequest[FullAssignmentTree])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, AssignmentValidationResult] = {
    val assignmentRequest: FullAssignmentTree = parsedRequest.fullViewTree

    val reassignmentId = parsedRequest.reassignmentId
    val sourcePSId = assignmentRequest.sourceSynchronizer
    val targetSnapshot = Target(parsedRequest.snapshot).map(_.ipsSnapshot)
    val isReassigningParticipant = assignmentRequest.isReassigningParticipant(participantId)

    for {
      commonValidationResult <- EitherT.right(
        performCommonValidations(parsedRequest, activenessF)
      )

      reassigningParticipantValidationResult <- unassignmentDataE match {
        case _ if !isReassigningParticipant =>
          EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
            ReassigningParticipantValidationResult(Nil)
          )

        case Right(unassignmentData) =>
          performValidationForReassigningParticipants(
            parsedRequest,
            unassignmentData,
          )

        case Left(_: ReassignmentCompleted) =>
          EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
            ReassigningParticipantValidationResult(Seq(AssignmentCompleted(reassignmentId)))
          )

        // In phase 7, the assignmentData is written, and the time of completion is recorded a bit later in the conflict detector.
        // Ideally, we would remove these two steps of writing data in the database. However, the serializable contract is not currently available
        // in the conflict detector. One solution could be to enrich UnassignmentCommit and AssignmentCommit with the serializable contract,
        // allowing everything to be written during the conflict detector phase, thereby removing the need for AssignmentStartingBeforeUnassignment.
        // Alternatively, we could wait until the contract is removed from the store and then write the assignment data and the completion time in the conflict detector.
        case Left(_: AssignmentStartingBeforeUnassignment) =>
          EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
            ReassigningParticipantValidationResult(Seq(UnassignmentDataNotFound(reassignmentId)))
          )

        case Left(_: UnknownReassignmentId) =>
          EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
            ReassigningParticipantValidationResult(Seq(UnassignmentDataNotFound(reassignmentId)))
          )
      }

      hostedConfirmingReassigningParties <- EitherT.right(
        if (isReassigningParticipant)
          targetSnapshot.unwrap.canConfirm(
            participantId,
            parsedRequest.fullViewTree.confirmingParties,
          )
        else
          FutureUnlessShutdown.pure(Set.empty[LfPartyId])
      )

    } yield AssignmentValidationResult(
      rootHash = assignmentRequest.rootHash,
      contracts = assignmentRequest.contracts,
      submitterMetadata = assignmentRequest.submitterMetadata,
      reassignmentId = reassignmentId,
      sourcePSId = sourcePSId,
      isReassigningParticipant = isReassigningParticipant,
      hostedConfirmingReassigningParties = hostedConfirmingReassigningParties,
      commonValidationResult = commonValidationResult,
      reassigningParticipantValidationResult = reassigningParticipantValidationResult,
    )
  }

  def performCommonValidations(
      parsedRequest: ParsedReassignmentRequest[FullAssignmentTree],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[reassignment.AssignmentValidationResult.CommonValidationResult] = {
    val topologySnapshot = Target(parsedRequest.snapshot.ipsSnapshot)
    val assignmentRequest: FullAssignmentTree = parsedRequest.fullViewTree

    val contractAuthenticationResultF =
      ReassignmentValidation.authenticateContractAndStakeholders(
        contractValidator,
        assignmentRequest,
      )

    for {
      activenessResult <- activenessF

      submitterCheckResult <-
        ReassignmentValidation
          .checkSubmitter(
            assignmentRequest.reassignmentRef,
            topologySnapshot = topologySnapshot,
            submitter = assignmentRequest.submitter,
            participantId = assignmentRequest.submitterMetadata.submittingParticipant,
            stakeholders = assignmentRequest.stakeholders.all,
          )
          .value
          .map(_.swap.toOption)

      reassignmentIdResult = validateReassignmentId(parsedRequest.fullViewTree)

      participantSignatureVerificationResult <- AuthenticationValidator.verifyViewSignature(
        parsedRequest
      )

    } yield AssignmentValidationResult.CommonValidationResult(
      activenessResult = activenessResult,
      participantSignatureVerificationResult = participantSignatureVerificationResult,
      contractAuthenticationResultF = contractAuthenticationResultF,
      submitterCheckResult = submitterCheckResult,
      reassignmentIdResult = reassignmentIdResult,
    )
  }

  def performValidationForReassigningParticipants(
      parsedRequest: ParsedReassignmentRequest[FullAssignmentTree],
      unassignmentData: UnassignmentData,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    ReassigningParticipantValidationResult,
  ] = {
    val assignmentRequest = parsedRequest.fullViewTree
    val assignmentRequestTs = parsedRequest.requestTimestamp

    for {
      // TODO(i26479): Check that reassignmentData.unassignmentRequest.targetTimestamp is in the past
      exclusivityTimeoutError <- AssignmentValidation.checkExclusivityTimeout(
        reassignmentCoordination,
        targetPSId,
        staticSynchronizerParameters,
        unassignmentData,
        assignmentRequestTs,
        assignmentRequest.submitter,
        parsedRequest.reassignmentId,
      )

      reassignmentDataResult <- EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
        validateAssignmentRequestAgainstUnassignmentData(
          assignmentRequest,
          unassignmentData,
        )
      )

    } yield ReassigningParticipantValidationResult(
      exclusivityTimeoutError.toList ++ reassignmentDataResult
    )
  }

  private def validateReassignmentId(
      fullViewTree: FullAssignmentTree
  ): Option[AssignmentValidationError.InconsistentReassignmentId] =
    Option.unless(fullViewTree.isReassignmentIdValid) {
      AssignmentValidationError.InconsistentReassignmentId(fullViewTree.reassignmentId)
    }

  private def validateAssignmentRequestAgainstUnassignmentData(
      assignmentRequest: FullAssignmentTree,
      unassignmentData: UnassignmentData,
  ): Seq[ReassignmentValidationError] = {

    val reassignmentId = unassignmentData.reassignmentId

    val reassigningParticipants = Validated.condNec(
      unassignmentData.reassigningParticipants == assignmentRequest.reassigningParticipants,
      (),
      ReassignmentValidationError.ReassigningParticipantsMismatch(
        ReassignmentRef(reassignmentId),
        expected = unassignmentData.reassigningParticipants,
        declared = assignmentRequest.reassigningParticipants,
      ),
    )

    val contract = Validated.condNec(
      unassignmentData.contractsBatch.contracts.toSeq == assignmentRequest.contracts.contracts.toSeq,
      (),
      ContractDataMismatch(reassignmentId),
    )

    val reassignmentCounter = {
      val declaredCounters = assignmentRequest.contracts.contractIdCounters
      val expectedCounters = unassignmentData.contractsBatch.contractIdCounters
      Validated.condNec(
        declaredCounters == expectedCounters,
        (),
        InconsistentReassignmentCounters(
          reassignmentId,
          declaredCounters.diff(expectedCounters).toMap,
          expectedCounters.diff(declaredCounters).toMap,
        ),
      )
    }

    val unassignmentTs = {
      val declaredUnassignmentTs = assignmentRequest.tree.commonData.tryUnwrap.unassignmentTs
      val expectedUnassignmentTs = unassignmentData.unassignmentTs
      Validated.condNec(
        declaredUnassignmentTs == expectedUnassignmentTs,
        (),
        UnassignmentTimestampMismatch(
          reassignmentId,
          declaredUnassignmentTs,
          expectedUnassignmentTs,
        ),
      )
    }

    Seq(
      reassigningParticipants,
      contract,
      reassignmentCounter,
      unassignmentTs,
    ).sequence_.fold(_.toList, _ => Nil)
  }
}

private[reassignment] sealed trait AssignmentProcessorError extends ReassignmentProcessorError

object AssignmentValidation {

  /** Checks whether the submitter is either the initiator of the unassignment or the exclusivity
    * timeout has elapsed.
    */
  def checkExclusivityTimeout(
      reassignmentCoordination: ReassignmentCoordination,
      targetPSId: Target[PhysicalSynchronizerId],
      staticSynchronizerParameters: Target[StaticSynchronizerParameters],
      unassignmentData: UnassignmentData,
      requestTimestamp: CantonTimestamp,
      submitter: LfPartyId,
      reassignmentId: ReassignmentId,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Option[
    ReassignmentValidationError
  ]] = {
    val targetTimestamp = unassignmentData.targetTimestamp
    for {
      // TODO(i26479): Check that reassignmentData.unassignmentRequest.targetTimestamp is in the past
      cryptoSnapshotTargetTs <- reassignmentCoordination
        .cryptoSnapshot(
          /*
          `targetPSId` can differ from `unassignmentData.targetPSId` if the target synchronizer is upgraded
          between unassignment and assignment.
           */
          targetPSId,
          staticSynchronizerParameters,
          targetTimestamp,
        )
        .map(_.map(_.ipsSnapshot))

      exclusivityLimit <- ProcessingSteps
        .getAssignmentExclusivity(
          cryptoSnapshotTargetTs,
          targetTimestamp,
        )
        .leftMap[ReassignmentProcessorError](
          ReassignmentParametersError(targetPSId.unwrap, _)
        )

      validationError = Option.when(
        requestTimestamp < exclusivityLimit.unwrap && unassignmentData.submitterMetadata.submitter != submitter
      )(
        NonInitiatorSubmitsBeforeExclusivityTimeout(
          reassignmentId,
          unassignmentData.submitterMetadata.submitter,
          currentTimestamp = requestTimestamp,
          timeout = exclusivityLimit,
        )
      )

    } yield validationError
  }

  final case class NoReassignmentData(
      reassignmentId: ReassignmentId,
      lookupError: ReassignmentStore.ReassignmentLookupError,
  ) extends AssignmentProcessorError {
    override def message: String =
      s"Cannot find reassignment data for reassignment `$reassignmentId`: ${lookupError.cause}"
  }

  final case class NoParticipantForReceivingParty(reassignmentId: ReassignmentId, party: LfPartyId)
      extends AssignmentProcessorError {
    override def message: String =
      s"Cannot assign `$reassignmentId` because $party is not active"
  }

  final case class UnexpectedSynchronizer(
      reassignmentId: ReassignmentId,
      targetSynchronizerId: PhysicalSynchronizerId,
      receivedOn: PhysicalSynchronizerId,
  ) extends AssignmentProcessorError {
    override def message: String =
      s"Cannot assign `$reassignmentId`: expecting synchronizer `$targetSynchronizerId` but received on `$receivedOn`"
  }
}
