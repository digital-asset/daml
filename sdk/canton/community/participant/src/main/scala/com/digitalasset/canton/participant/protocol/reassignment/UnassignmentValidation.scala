// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import cats.syntax.functor.*
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullUnassignmentTree,
  ReassignmentRef,
  UnassignmentData,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.GetTopologyAtTimestamp
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationResult.ReassigningParticipantValidationResult
import com.digitalasset.canton.participant.protocol.validation.AuthenticationValidator
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.ExecutionContext

sealed trait UnassignmentValidation {

  def perform(parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, UnassignmentValidationResult]

  protected def performCommonValidations(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      contractValidator: ContractValidator,
      activenessF: FutureUnlessShutdown[ActivenessResult],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[UnassignmentValidationResult.CommonValidationResult] = {
    val fullTree = parsedRequest.fullViewTree
    val sourceTopologySnapshot = Source(parsedRequest.snapshot.ipsSnapshot)
    val authenticationResultET =
      ReassignmentValidation.authenticateContractAndStakeholders(contractValidator, fullTree)

    for {
      activenessResult <- activenessF
      authenticationErrorO <- AuthenticationValidator.verifyViewSignature(parsedRequest)

      // The contract instance is validated in the `authenticationResultET`, this is why we can use it here.
      expectedStakeholders = fullTree.contracts.stakeholders

      submitterCheckResult <-
        ReassignmentValidation
          .checkSubmitter(
            ReassignmentRef(fullTree.contracts.contractIds.toSet),
            topologySnapshot = sourceTopologySnapshot,
            submitter = fullTree.submitter,
            participantId = fullTree.submitterMetadata.submittingParticipant,
            stakeholders = expectedStakeholders.all,
          )
          .value
          .map(_.swap.toOption)

    } yield UnassignmentValidationResult.CommonValidationResult(
      activenessResult = activenessResult,
      participantSignatureVerificationResult = authenticationErrorO,
      contractAuthenticationResultF = authenticationResultET,
      submitterCheckResult = submitterCheckResult,
    )
  }
}

object UnassignmentValidation {

  def apply(
      isReassigningParticipant: Boolean,
      participantId: ParticipantId,
      contractValidator: ContractValidator,
      activenessF: FutureUnlessShutdown[ActivenessResult],
      getTopologyAtTs: GetTopologyAtTimestamp,
  ): UnassignmentValidation =
    if (isReassigningParticipant)
      ReassigningParticipantUnassignmentValidator(
        participantId,
        contractValidator,
        activenessF,
        getTopologyAtTs,
      )
    else NonReassigningParticipantUnassignmentValidator(contractValidator, activenessF)
}

private final case class NonReassigningParticipantUnassignmentValidator(
    contractValidator: ContractValidator,
    activenessF: FutureUnlessShutdown[ActivenessResult],
) extends UnassignmentValidation {
  def perform(parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, UnassignmentValidationResult] =
    EitherT.right(
      performCommonValidations(parsedRequest, contractValidator, activenessF)
        .map { commonValidationResult =>
          UnassignmentValidationResult(
            unassignmentData =
              UnassignmentData(parsedRequest.fullViewTree, parsedRequest.requestTimestamp),
            rootHash = parsedRequest.rootHash,
            commonValidationResult = commonValidationResult,
            // The below fields all pertain to reassigning participants.
            reassigningParticipantValidationResult = ReassigningParticipantValidationResult(Nil),
            assignmentExclusivity = None,
            hostedConfirmingReassigningParties = Set.empty,
          )
        }
    )
}

private final case class ReassigningParticipantUnassignmentValidator(
    participantId: ParticipantId,
    contractValidator: ContractValidator,
    activenessF: FutureUnlessShutdown[ActivenessResult],
    getTopologyAtTs: GetTopologyAtTimestamp,
) extends UnassignmentValidation {

  def perform(parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, UnassignmentValidationResult] = {
    val fullTree = parsedRequest.fullViewTree
    val sourceTopology = Source(parsedRequest.snapshot.ipsSnapshot)

    for {
      commonValidationResult <- EitherT.right(
        performCommonValidations(
          parsedRequest,
          contractValidator,
          activenessF,
        )
      )
      targetTopologyO <- getTopologyAtTs.maybeAwaitTopologySnapshot(
        fullTree.targetSynchronizer,
        fullTree.targetTimestamp,
      )
      resultsWithTargetTopology <- targetTopologyO match {
        case Some(targetTopology) =>
          performValidationWithTargetTopology(parsedRequest, targetTopology)
        case None =>
          val results = (ReassigningParticipantValidationResult.TargetTimestampTooFarInFuture, None)
          EitherT.right(FutureUnlessShutdown.pure(results))
      }
      (reassigningParticipantValidationResult, assignmentExclusivity) = resultsWithTargetTopology
      hostedConfirmingReassigningParties <- EitherT.right(
        sourceTopology.unwrap
          .canConfirm(participantId, parsedRequest.fullViewTree.confirmingParties)
      )

    } yield UnassignmentValidationResult(
      unassignmentData =
        UnassignmentData(parsedRequest.fullViewTree, parsedRequest.requestTimestamp),
      rootHash = parsedRequest.rootHash,
      commonValidationResult = commonValidationResult,
      // The below fields all pertain to reassigning participants.
      reassigningParticipantValidationResult = reassigningParticipantValidationResult,
      assignmentExclusivity = assignmentExclusivity,
      hostedConfirmingReassigningParties = hostedConfirmingReassigningParties,
    )
  }

  private def performValidationWithTargetTopology(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      targetTopology: Target[TopologySnapshot],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    (ReassigningParticipantValidationResult, Option[Target[CantonTimestamp]]),
  ] = for {
    reassigningParticipantValidationResult <- performValidationForReassigningParticipants(
      parsedRequest,
      targetTopology,
    )
    fullTree = parsedRequest.fullViewTree
    assignmentExclusivity <- ProcessingSteps
      .getAssignmentExclusivity(targetTopology, fullTree.targetTimestamp)
      .leftMap[ReassignmentProcessorError](
        ReassignmentParametersError(fullTree.targetSynchronizer.unwrap, _)
      )
  } yield (reassigningParticipantValidationResult, Some(assignmentExclusivity))

  private def performValidationForReassigningParticipants(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      targetTopology: Target[TopologySnapshot],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    ReassigningParticipantValidationResult,
  ] = {
    val sourceTopology = Source(parsedRequest.snapshot.ipsSnapshot)
    val fullTree = parsedRequest.fullViewTree
    val expectedStakeholders = fullTree.contracts.stakeholders
    val packageIds = fullTree.contracts.packageIds

    EitherT.right(
      new UnassignmentValidationReassigningParticipant(
        sourceTopology,
        targetTopology,
      )(fullTree)
        .check(expectedStakeholders, packageIds)
        .value
        .map(_.swap.toSeq)
        .map(ReassigningParticipantValidationResult(_))
    )
  }
}
