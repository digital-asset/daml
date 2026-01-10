// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.BaseUnassignmentValidator.BaseReassignmentValidation
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.ReassigningParticipantsMismatch
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidation.ValidationErrorOr
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationError.PackageIdUnknownOrUnvetted
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationResult.ReassigningParticipantValidationResult
import com.digitalasset.canton.participant.protocol.submission.UsableSynchronizers
import com.digitalasset.canton.participant.protocol.validation.AuthenticationValidator
import com.digitalasset.canton.protocol.{LfContractId, Stakeholders}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{LfPackageId, LfPartyId}

import scala.concurrent.ExecutionContext

sealed trait UnassignmentValidation {

  def perform(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  ): ValidationErrorOr[UnassignmentValidationResult]

}

object UnassignmentValidation {

  type ValidationErrorOr[A] = EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    A,
  ]

  def apply(
      isReassigningParticipant: Boolean,
      participantId: ParticipantId,
      contractValidator: ContractValidator,
      getTopologyAtTs: GetTopologyAtTimestamp,
  )(implicit ec: ExecutionContext, traceContext: TraceContext): UnassignmentValidation =
    if (isReassigningParticipant)
      new ReassigningParticipantUnassignmentValidator(
        participantId,
        contractValidator,
        getTopologyAtTs,
      )
    else
      new NonReassigningParticipantUnassignmentValidator(
        participantId,
        contractValidator,
        getTopologyAtTs,
      )
}

object BaseUnassignmentValidator {
  final case class BaseReassignmentValidation(
      hostedConfirmingReassigningParties: Set[LfPartyId],
      assignmentExclusivity: Option[Target[CantonTimestamp]],
      reassigningParticipantValidationResult: UnassignmentValidationResult.ReassigningParticipantValidationResult,
  )
}

private trait BaseUnassignmentValidator extends UnassignmentValidation {
  def participantId: ParticipantId
  def contractValidator: ContractValidator
  def getTopologyAtTs: GetTopologyAtTimestamp
  implicit def executionContext: ExecutionContext
  implicit def traceContext: TraceContext

  private def checkSubmitterCheckResult(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree]
  ): ValidationErrorOr[Option[ReassignmentValidationError]] = {

    val fullTree = parsedRequest.fullViewTree

    EitherT.right(
      ReassignmentValidation
        .checkSubmitter(
          ReassignmentRef(fullTree.contracts.contractIds.toSet),
          topologySnapshot = Source(parsedRequest.snapshot.ipsSnapshot),
          submitter = fullTree.submitter,
          participantId = fullTree.submitterMetadata.submittingParticipant,
          stakeholders = fullTree.contracts.stakeholders.all,
        )
        .value
        .map(_.swap.toOption)
    )
  }

  private def checkCommonValidationResult(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  ): ValidationErrorOr[UnassignmentValidationResult.CommonValidationResult] = for {
    activenessResult <- EitherT.right(activenessF)
    participantSignatureVerificationResult <- EitherT.right(
      AuthenticationValidator.verifyViewSignature(parsedRequest)
    )
    contractAuthenticationResultF = ReassignmentValidation.authenticateContractAndStakeholders(
      contractValidator,
      parsedRequest.fullViewTree,
    )
    submitterCheckResult <- checkSubmitterCheckResult(parsedRequest)
  } yield UnassignmentValidationResult.CommonValidationResult(
    activenessResult,
    participantSignatureVerificationResult,
    contractAuthenticationResultF,
    submitterCheckResult,
  )

  protected def checkPackagesVetted(
      stakeholders: Stakeholders,
      contractIds: Set[LfContractId],
      packageIds: Set[LfPackageId],
      topologySnapshot: TopologySnapshot,
      synchronizerId: PhysicalSynchronizerId,
  ): ValidationErrorOr[Option[ReassignmentValidationError]] =
    EitherT.right(
      UsableSynchronizers
        .checkPackagesVetted(
          synchronizerId,
          topologySnapshot,
          stakeholders.all.map(_ -> packageIds).toMap,
          topologySnapshot.referenceTime,
        )
        .value
        .map(
          _.swap.toOption.map(u =>
            PackageIdUnknownOrUnvetted(contractIds, u.unknownTo, synchronizerId)
          )
        )
    )

  def checkBaseReassignmentValidation(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree]
  ): ValidationErrorOr[BaseReassignmentValidation]

  def perform(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  ): ValidationErrorOr[UnassignmentValidationResult] =
    for {
      commonValidationResult <- checkCommonValidationResult(parsedRequest, activenessF)
      reassignmentValidation <- checkBaseReassignmentValidation(parsedRequest)
    } yield UnassignmentValidationResult(
      unassignmentData =
        UnassignmentData(parsedRequest.fullViewTree, parsedRequest.requestTimestamp),
      rootHash = parsedRequest.rootHash,
      hostedConfirmingReassigningParties =
        reassignmentValidation.hostedConfirmingReassigningParties,
      assignmentExclusivity = reassignmentValidation.assignmentExclusivity,
      commonValidationResult = commonValidationResult,
      reassigningParticipantValidationResult =
        reassignmentValidation.reassigningParticipantValidationResult,
    )

}

private class NonReassigningParticipantUnassignmentValidator(
    val participantId: ParticipantId,
    val contractValidator: ContractValidator,
    val getTopologyAtTs: GetTopologyAtTimestamp,
)(implicit
    override val executionContext: ExecutionContext,
    override val traceContext: TraceContext,
) extends BaseUnassignmentValidator {
  override def checkBaseReassignmentValidation(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree]
  ): ValidationErrorOr[BaseReassignmentValidation] =
    EitherT.pure(
      BaseReassignmentValidation(
        hostedConfirmingReassigningParties = Set.empty,
        assignmentExclusivity = None,
        reassigningParticipantValidationResult =
          UnassignmentValidationResult.ReassigningParticipantValidationResult(Nil),
      )
    )
}

private class ReassigningParticipantUnassignmentValidator(
    val participantId: ParticipantId,
    val contractValidator: ContractValidator,
    val getTopologyAtTs: GetTopologyAtTimestamp,
)(implicit
    override val executionContext: ExecutionContext,
    override val traceContext: TraceContext,
) extends BaseUnassignmentValidator {

  private def checkHostedConfirmingReassigningParties(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree]
  ): ValidationErrorOr[Set[LfPartyId]] = EitherT.right(
    parsedRequest.snapshot.ipsSnapshot
      .canConfirm(participantId, parsedRequest.fullViewTree.confirmingParties)
  )

  private def checkAssignmentExclusivity(
      fullTree: FullUnassignmentTree,
      targetTopologyO: Option[Target[TopologySnapshot]],
  ): ValidationErrorOr[Option[Target[CantonTimestamp]]] =
    targetTopologyO match {
      case Some(targetTopology) =>
        ProcessingSteps
          .getAssignmentExclusivity(targetTopology, fullTree.targetTimestamp)
          .map(Option(_))
          .leftMap[ReassignmentProcessorError](
            ReassignmentParametersError(fullTree.targetSynchronizer.unwrap, _)
          )
      case None =>
        EitherT.right(
          FutureUnlessShutdown.pure[Option[Target[CantonTimestamp]]](None)
        )
    }

  // check the package of the template is vetted
  private def checkTargetPackagesVetted(
      fullTree: FullUnassignmentTree,
      targetTopology: Target[TopologySnapshot],
  ): ValidationErrorOr[Option[ReassignmentValidationError]] =
    checkPackagesVetted(
      stakeholders = fullTree.contracts.stakeholders,
      // TODO(#29199): Use target package IDs
      contractIds = fullTree.contracts.contractIds.toSet,
      packageIds = fullTree.contracts.packageIds,
      topologySnapshot = targetTopology.unwrap,
      synchronizerId = fullTree.targetSynchronizer.unwrap,
    )

  // check the reassigning participants from the request match the computed reassigning participants
  // check all stakeholders are hosted on active participants
  // check the recipients from the request match the computed recipients
  private def checkReassigningParticipants(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      targetTopology: Target[TopologySnapshot],
  ): ValidationErrorOr[Option[ReassignmentValidationError]] =
    EitherT
      .right(
        new ReassigningParticipantsComputation(
          parsedRequest.fullViewTree.contracts.stakeholders,
          Source(parsedRequest.snapshot.ipsSnapshot),
          targetTopology,
        ).compute.value
      )
      .map {
        case Right(contractReassigningParticipants) =>
          val fullViewTree = parsedRequest.fullViewTree
          val requestReassigningParticipants = fullViewTree.reassigningParticipants
          Option.when(contractReassigningParticipants != requestReassigningParticipants)(
            ReassigningParticipantsMismatch(
              reassignmentRef =
                ReassignmentRef.ContractIdRef(fullViewTree.contracts.contractIds.toSet),
              expected = contractReassigningParticipants,
              declared = requestReassigningParticipants,
            )
          )
        case Left(rve) =>
          Some(rve)
      }

  private def computeReassigningParticipantValidationResult(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      targetTopologyO: Option[Target[TopologySnapshot]],
  ): ValidationErrorOr[ReassigningParticipantValidationResult] =
    targetTopologyO match {
      case Some(targetTopology) =>
        for {
          participantsErrors <- checkReassigningParticipants(parsedRequest, targetTopology)
          vettingErrors <- checkTargetPackagesVetted(parsedRequest.fullViewTree, targetTopology)
        } yield {
          ReassigningParticipantValidationResult(
            participantsErrors.toList ++ vettingErrors.toList
          )
        }
      case None =>
        EitherT.rightT(ReassigningParticipantValidationResult.TargetTimestampTooFarInFuture)
    }

  override def checkBaseReassignmentValidation(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree]
  ): ValidationErrorOr[BaseReassignmentValidation] =
    for {

      targetTopologyO <- getTopologyAtTs.maybeAwaitTopologySnapshot(
        parsedRequest.fullViewTree.targetSynchronizer,
        parsedRequest.fullViewTree.targetTimestamp,
      )

      hostedConfirmingReassigningParties <- checkHostedConfirmingReassigningParties(parsedRequest)
      assignmentExclusivity <- checkAssignmentExclusivity(
        parsedRequest.fullViewTree,
        targetTopologyO,
      )
      reassigningParticipantValidationResult <- computeReassigningParticipantValidationResult(
        parsedRequest,
        targetTopologyO,
      )
    } yield BaseReassignmentValidation(
      hostedConfirmingReassigningParties,
      assignmentExclusivity,
      reassigningParticipantValidationResult,
    )

}
