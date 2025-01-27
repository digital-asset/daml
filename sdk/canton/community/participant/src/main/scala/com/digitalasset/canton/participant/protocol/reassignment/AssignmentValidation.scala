// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.implicits.toFunctorOps
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{SyncCryptoApi, SynchronizerSnapshotSyncCryptoApi}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidationError.InvalidUnassignmentResult.DeliveredUnassignmentResultError
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidationError.{
  ContractDataMismatch,
  InconsistentReassignmentCounter,
  NonInitiatorSubmitsBeforeExclusivityTimeout,
  ReassignmentDataCompleted,
  UnassignmentDataNotFound,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.validation.AuthenticationValidator
import com.digitalasset.canton.participant.protocol.{EngineController, ProcessingSteps}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ReassignmentStore.{
  AssignmentStartingBeforeUnassignment,
  ReassignmentCompleted,
  UnknownReassignmentId,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.ExecutionContext

private[reassignment] class AssignmentValidation(
    synchronizerId: Target[SynchronizerId],
    staticSynchronizerParameters: Target[StaticSynchronizerParameters],
    participantId: ParticipantId,
    reassignmentCoordination: ReassignmentCoordination,
    engine: DAMLe,
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
      engineController: EngineController,
  )(parsedRequest: ParsedReassignmentRequest[FullAssignmentTree])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, AssignmentValidationResult] = {
    val assignmentRequest: FullAssignmentTree = parsedRequest.fullViewTree
    val assignmentRequestTs = parsedRequest.requestTimestamp

    val reassignmentId = assignmentRequest.unassignmentResultEvent.reassignmentId
    val targetSnapshot = targetCrypto.map(_.ipsSnapshot)
    val isReassigningParticipant = assignmentRequest.isReassigningParticipant(participantId)

    for {
      validationResult <- EitherT.right(
        performValidation(
          targetCrypto,
          activenessF,
          engineController,
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
            targetCrypto,
          )
        case Left(_: ReassignmentCompleted) =>
          EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
            Seq(ReassignmentDataCompleted(reassignmentId): ReassignmentValidationError)
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
      contract = assignmentRequest.contract,
      reassignmentCounter = assignmentRequest.reassignmentCounter,
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
      engineController: EngineController,
  )(parsedRequest: ParsedReassignmentRequest[FullAssignmentTree])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[AssignmentValidationResult.ValidationResult] = {
    val assignmentRequest: FullAssignmentTree = parsedRequest.fullViewTree

    val reassignmentId = assignmentRequest.unassignmentResultEvent.reassignmentId
    val targetSnapshot = targetCrypto.map(_.ipsSnapshot)

    // We perform the stakeholders check asynchronously so that we can complete the pending request
    // in the Phase37Synchronizer without waiting for it, thereby allowing us to concurrently receive a
    // mediator verdict.
    val stakeholdersCheckResultET = new ReassignmentValidation(engine)
      .checkMetadata(
        assignmentRequest,
        getEngineAbortStatus = () => engineController.abortStatus,
      )

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
      targetCrypto: Target[SynchronizerSnapshotSyncCryptoApi],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Seq[ReassignmentValidationError]] = {
    val sourceSynchronizer = unassignmentData.unassignmentRequest.sourceSynchronizer
    val unassignmentTs = unassignmentData.unassignmentTs
    val targetSnapshot = targetCrypto.map(_.ipsSnapshot)
    for {
      sourceStaticSynchronizerParam <- reassignmentCoordination
        .getStaticSynchronizerParameter(sourceSynchronizer)
      _ready <- {
        logger.info(
          s"Waiting for topology state at $unassignmentTs on unassignment synchronizer $sourceSynchronizer ..."
        )
        reassignmentCoordination
          .awaitUnassignmentTimestamp(
            sourceSynchronizer,
            sourceStaticSynchronizerParam,
            unassignmentTs,
          )
      }

      sourceCrypto <- reassignmentCoordination
        .cryptoSnapshot(
          sourceSynchronizer,
          sourceStaticSynchronizerParam,
          unassignmentTs,
        )

      targetTimeProof = unassignmentData.unassignmentRequest.targetTimeProof.timestamp

      // TODO(i12926): Check that reassignmentData.unassignmentRequest.targetTimeProof.timestamp is in the past
      cryptoSnapshotAtTimeProof <- reassignmentCoordination
        .cryptoSnapshot(
          unassignmentData.targetSynchronizer,
          staticSynchronizerParameters,
          targetTimeProof,
        )

      exclusivityLimit <- ProcessingSteps
        .getAssignmentExclusivity(
          cryptoSnapshotAtTimeProof.map(_.ipsSnapshot),
          targetTimeProof,
        )
        .leftMap[ReassignmentProcessorError](ReassignmentParametersError(synchronizerId.unwrap, _))

      // TODO(i12926): Validate the shipped unassignment result w.r.t. stakeholders

      reassignmentDataResult <- EitherT.right(
        validateUnassignmentData(
          unassignmentData,
          assignmentRequest,
          assignmentRequestTs,
          exclusivityLimit,
          sourceCrypto,
          targetSnapshot,
        )
      )

    } yield reassignmentDataResult
  }

  private def validateUnassignmentData(
      unassignmentData: UnassignmentData,
      assignmentRequest: FullAssignmentTree,
      assignmentRequestTs: CantonTimestamp,
      exclusivityLimit: Target[CantonTimestamp],
      sourceTopology: Source[SyncCryptoApi],
      targetTopology: Target[TopologySnapshot],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[ReassignmentValidationError]] = {
    // TODO(i12926): Validate that the unassignment result received matches the unassignment result in reassignmentData

    val UnassignmentData(
      unassignmentTs,
      _unassignmentRequestCounter,
      unassignmentRequest,
      unassignmentDecisionTime,
      _unassignmentResult,
    ) = unassignmentData

    val reassignmentId = assignmentRequest.unassignmentResultEvent.reassignmentId

    val error1 =
      if (unassignmentRequest.reassigningParticipants == assignmentRequest.reassigningParticipants)
        Nil
      else
        Seq(
          ReassignmentValidationError.ReassigningParticipantsMismatch(
            ReassignmentRef(reassignmentId),
            expected = unassignmentData.unassignmentRequest.reassigningParticipants,
            declared = assignmentRequest.reassigningParticipants,
          )
        )

    val error2 =
      if (unassignmentRequest.contract == assignmentRequest.contract) Nil
      else
        Seq(
          ContractDataMismatch(reassignmentId)
        )

    val error3 =
      if (
        assignmentRequestTs >= exclusivityLimit.unwrap || unassignmentRequest.submitter == assignmentRequest.submitter
      ) Nil
      else
        Seq(
          NonInitiatorSubmitsBeforeExclusivityTimeout(
            reassignmentId,
            assignmentRequest.submitter,
            currentTimestamp = assignmentRequestTs,
            timeout = exclusivityLimit,
          )
        )

    // reassignment counter is the same in unassignment and assignment requests
    val error4 =
      if (assignmentRequest.reassignmentCounter == unassignmentData.reassignmentCounter) Nil
      else
        Seq(
          InconsistentReassignmentCounter(
            reassignmentId,
            assignmentRequest.reassignmentCounter,
            unassignmentData.reassignmentCounter,
          )
        )

    for {
      error5 <- DeliveredUnassignmentResultValidation(
        unassignmentRequest = unassignmentRequest,
        unassignmentRequestTs = unassignmentTs,
        unassignmentDecisionTime = unassignmentDecisionTime,
        sourceTopology = sourceTopology,
        targetTopology = targetTopology,
      )(assignmentRequest.unassignmentResultEvent).validate.leftMap { err =>
        DeliveredUnassignmentResultError(reassignmentId, err.error).reported()
      }.value

    } yield error1 ++ error2 ++ error3 ++ error4 ++ error5.swap.toSeq

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

  final case class UnassignmentIncomplete(
      reassignmentId: ReassignmentId,
      participant: ParticipantId,
  ) extends AssignmentProcessorError {
    override def message: String =
      s"Cannot assign `$reassignmentId` because unassignment is incomplete"
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
