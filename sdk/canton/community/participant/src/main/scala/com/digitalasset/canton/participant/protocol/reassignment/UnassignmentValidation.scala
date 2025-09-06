// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{FullUnassignmentTree, ReassignmentRef, UnassignmentData}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationResult.ReassigningParticipantValidationResult
import com.digitalasset.canton.participant.protocol.validation.AuthenticationValidator
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractAuthenticator
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.ExecutionContext

private[reassignment] class UnassignmentValidation(
    participantId: ParticipantId,
    contractAuthenticator: ContractAuthenticator,
)(implicit val ec: ExecutionContext)
    extends ReassignmentValidation[
      FullUnassignmentTree,
      UnassignmentValidationResult.CommonValidationResult,
      UnassignmentValidationResult.ReassigningParticipantValidationResult,
    ] {

  /** @param targetTopology
    *   Defined if and only if the participant is reassigning
    */
  def perform(
      targetTopology: Option[Target[TopologySnapshot]],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  )(parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, UnassignmentValidationResult] = {
    val fullTree = parsedRequest.fullViewTree
    val sourceTopology = Source(parsedRequest.snapshot.ipsSnapshot)
    val isReassigningParticipant = fullTree.isReassigningParticipant(participantId)

    for {
      commonValidationResult <- EitherT.right(
        performCommonValidations(
          parsedRequest,
          activenessF,
        )
      )

      // `targetTopology` is defined only for reassigning participants
      reassigningParticipantValidationResult <- targetTopology match {
        case Some(targetTopology) =>
          performValidationForReassigningParticipants(parsedRequest, targetTopology)
        case None =>
          EitherT.right(FutureUnlessShutdown.pure(ReassigningParticipantValidationResult(Nil)))
      }

      hostedConfirmingReassigningParties <- EitherT.right(
        if (isReassigningParticipant)
          sourceTopology.unwrap.canConfirm(
            participantId,
            parsedRequest.fullViewTree.confirmingParties,
          )
        else
          FutureUnlessShutdown.pure(Set.empty[LfPartyId])
      )

      assignmentExclusivity <- targetTopology.traverse { targetTopology =>
        ProcessingSteps
          .getAssignmentExclusivity(targetTopology, fullTree.targetTimeProof.timestamp)
          .leftMap[ReassignmentProcessorError](
            ReassignmentParametersError(fullTree.targetSynchronizer.unwrap, _)
          )
      }

    } yield UnassignmentValidationResult(
      unassignmentData = UnassignmentData(fullTree, parsedRequest.requestTimestamp),
      rootHash = parsedRequest.rootHash,
      hostedConfirmingReassigningParties = hostedConfirmingReassigningParties,
      assignmentExclusivity = assignmentExclusivity,
      commonValidationResult = commonValidationResult,
      reassigningParticipantValidationResult = reassigningParticipantValidationResult,
    )
  }

  override def performCommonValidations(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[UnassignmentValidationResult.CommonValidationResult] = {
    val fullTree = parsedRequest.fullViewTree
    val sourceTopologySnapshot = Source(parsedRequest.snapshot.ipsSnapshot)
    val authenticationResultET =
      ReassignmentValidation.checkMetadata(contractAuthenticator, fullTree)

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
  override type ReassigningParticipantValidationData = Target[TopologySnapshot]

  override def performValidationForReassigningParticipants(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      targetTopology: Target[TopologySnapshot],
  )(implicit traceContext: TraceContext): EitherT[
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
