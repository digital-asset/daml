// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.{EitherT, Validated}
import cats.implicits.toFunctorOps
import cats.syntax.foldable.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.SynchronizerSnapshotSyncCryptoApi
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
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.validation.AuthenticationValidator
import com.digitalasset.canton.participant.protocol.{ContractAuthenticator, ProcessingSteps}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ReassignmentStore.{
  AssignmentStartingBeforeUnassignment,
  ReassignmentCompleted,
  UnknownReassignmentId,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

private[reassignment] class AssignmentValidation(
    synchronizerId: Target[SynchronizerId],
    staticSynchronizerParameters: Target[StaticSynchronizerParameters],
    participantId: ParticipantId,
    reassignmentCoordination: ReassignmentCoordination,
    contractAuthenticator: ContractAuthenticator,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  // TODO(#12926) Check what validations should be done for reassigning participants
  // TODO(#12926) Check what validations can be done here + ensure coverage (here means for both reassigningParticipant and non-reassigningParticipant)
  // TODO(#22119) Split this method in smaller chunks
  /** Validate the assignment request
    */
  def perform(
      targetCrypto: Target[SynchronizerSnapshotSyncCryptoApi],
      unassignmentDataE: Either[ReassignmentStore.ReassignmentLookupError, UnassignmentData],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  )(parsedRequest: ParsedReassignmentRequest[FullAssignmentTree])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, AssignmentValidationResult] = {
    val assignmentRequest: FullAssignmentTree = parsedRequest.fullViewTree
    val assignmentRequestTs = parsedRequest.requestTimestamp

    val reassignmentId = assignmentRequest.reassignmentId
    val targetSnapshot = targetCrypto.map(_.ipsSnapshot)
    val isReassigningParticipant = assignmentRequest.isReassigningParticipant(participantId)

    for {
      validationResult <- EitherT.right(
        performValidation(
          targetCrypto,
          activenessF,
        )(parsedRequest)
      )

      reassigningParticipantValidationResult <- unassignmentDataE match {
        case _ if !isReassigningParticipant =>
          EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
            Seq.empty[ReassignmentValidationError]
          )

        case Right(unassignmentData) =>
          validateAssignmentRequestForReassigningParticipant(
            unassignmentData,
            assignmentRequest,
            assignmentRequestTs,
          )

        case Left(_: ReassignmentCompleted) =>
          EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
            Seq(AssignmentCompleted(reassignmentId): ReassignmentValidationError)
          )
        // this a special case where we are retrying to reprocess an assignment data. It's safe to consider that the reassignment data is missing
        // because inserting AssignmentData is idempotent and detect modified values
        case Left(_: AssignmentStartingBeforeUnassignment) =>
          EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
            Seq(UnassignmentDataNotFound(reassignmentId): ReassignmentValidationError)
          )
        case Left(_: UnknownReassignmentId) =>
          EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
            Seq(
              UnassignmentDataNotFound(reassignmentId): ReassignmentValidationError
            )
          )
      }

      hostedStakeholders <- EitherT.right(
        targetSnapshot.unwrap
          .hostedOn(assignmentRequest.stakeholders.all, participantId)
          .map(_.keySet)
      )

    } yield AssignmentValidationResult(
      rootHash = assignmentRequest.rootHash,
      contracts = assignmentRequest.contracts,
      submitterMetadata = assignmentRequest.submitterMetadata,
      reassignmentId = reassignmentId,
      isReassigningParticipant = isReassigningParticipant,
      hostedStakeholders = hostedStakeholders,
      validationResult =
        validationResult.addValidationErrors(reassigningParticipantValidationResult),
    )
  }

  private def performValidation(
      targetCrypto: Target[SynchronizerSnapshotSyncCryptoApi],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  )(parsedRequest: ParsedReassignmentRequest[FullAssignmentTree])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[AssignmentValidationResult.ValidationResult] = {
    val assignmentRequest: FullAssignmentTree = parsedRequest.fullViewTree

    val reassignmentId = assignmentRequest.reassignmentId
    val targetSnapshot = targetCrypto.map(_.ipsSnapshot)

    val stakeholdersCheckResultET =
      new ReassignmentValidation(contractAuthenticator).checkMetadata(assignmentRequest)

    for {
      activenessResult <- activenessF

      submitterCheckResult <-
        ReassignmentValidation
          .checkSubmitter(
            ReassignmentRef(reassignmentId),
            topologySnapshot = targetSnapshot,
            submitter = assignmentRequest.submitter,
            participantId = assignmentRequest.submitterMetadata.submittingParticipant,
            stakeholders = assignmentRequest.stakeholders.all,
          )
          .value
          .map(_.swap.toSeq)

      authenticationErrorO <- AuthenticationValidator.verifyViewSignature(parsedRequest)

    } yield AssignmentValidationResult.ValidationResult(
      activenessResult = activenessResult,
      authenticationErrorO = authenticationErrorO,
      metadataResultET = stakeholdersCheckResultET,
      validationErrors = submitterCheckResult,
    )
  }

  private def validateAssignmentRequestForReassigningParticipant(
      unassignmentData: UnassignmentData,
      assignmentRequest: FullAssignmentTree,
      assignmentRequestTs: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Seq[ReassignmentValidationError]] = {
    val targetTimeProof = unassignmentData.unassignmentRequest.targetTimeProof.timestamp

    for {

      // TODO(i12926): Check that reassignmentData.unassignmentRequest.targetTimeProof.timestamp is in the past
      cryptoSnapshotTargetTs <- reassignmentCoordination
        .cryptoSnapshot(
          unassignmentData.targetSynchronizer,
          staticSynchronizerParameters,
          targetTimeProof,
        )
        .map(_.map(_.ipsSnapshot))

      exclusivityLimit <- ProcessingSteps
        .getAssignmentExclusivity(
          cryptoSnapshotTargetTs,
          targetTimeProof,
        )
        .leftMap[ReassignmentProcessorError](ReassignmentParametersError(synchronizerId.unwrap, _))

      reassignmentDataResult <- EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
        validateUnassignmentData(
          unassignmentData,
          assignmentRequest,
          assignmentRequestTs,
          exclusivityLimit,
        )
      )

    } yield reassignmentDataResult
  }

  private def validateUnassignmentData(
      unassignmentData: UnassignmentData,
      assignmentRequest: FullAssignmentTree,
      assignmentRequestTs: CantonTimestamp,
      exclusivityLimit: Target[CantonTimestamp],
  ): Seq[ReassignmentValidationError] = {
    // TODO(i12926): Validate that the unassignment result received matches the unassignment result in reassignmentData

    val UnassignmentData(
      reassignmentId,
      unassignmentRequest,
    ) = unassignmentData

    val reassigningParticipants = Validated.condNec(
      unassignmentRequest.reassigningParticipants == assignmentRequest.reassigningParticipants,
      (),
      ReassignmentValidationError.ReassigningParticipantsMismatch(
        ReassignmentRef(reassignmentId),
        expected = unassignmentData.unassignmentRequest.reassigningParticipants,
        declared = assignmentRequest.reassigningParticipants,
      ),
    )

    val contract = Validated.condNec(
      unassignmentRequest.contracts.contracts.toSeq == assignmentRequest.contracts.contracts.toSeq,
      (),
      ContractDataMismatch(reassignmentId),
    )

    val exclusivityTimeout = Validated.condNec(
      assignmentRequestTs >= exclusivityLimit.unwrap || unassignmentRequest.submitter == assignmentRequest.submitter,
      (),
      NonInitiatorSubmitsBeforeExclusivityTimeout(
        reassignmentId,
        assignmentRequest.submitter,
        currentTimestamp = assignmentRequestTs,
        timeout = exclusivityLimit,
      ),
    )

    val reassignmentCounter = {
      val declaredCounters = assignmentRequest.contracts.contractIdCounters
      val expectedCounters = unassignmentData.unassignmentRequest.contracts.contractIdCounters
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
    Seq(
      reassigningParticipants,
      contract,
      exclusivityTimeout,
      reassignmentCounter,
    ).sequence_.fold(_.toList, _ => Nil)
  }
}

private[reassignment] sealed trait AssignmentProcessorError extends ReassignmentProcessorError

object AssignmentValidation {
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
      targetSynchronizerId: SynchronizerId,
      receivedOn: SynchronizerId,
  ) extends AssignmentProcessorError {
    override def message: String =
      s"Cannot assign `$reassignmentId`: expecting synchronizer `$targetSynchronizerId` but received on `$receivedOn`"
  }
}
