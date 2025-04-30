// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import cats.syntax.traverse.*
import com.digitalasset.canton.data.{FullUnassignmentTree, ReassignmentRef}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.validation.AuthenticationValidator
import com.digitalasset.canton.participant.protocol.{ContractAuthenticator, ProcessingSteps}
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.ExecutionContext

private[reassignment] class UnassignmentValidation(
    participantId: ParticipantId,
    contractAuthenticator: ContractAuthenticator,
) {

  /** @param targetTopology
    *   Defined if and only if the participant is reassigning
    */
  def perform(
      sourceTopology: Source[TopologySnapshot],
      targetTopology: Option[Target[TopologySnapshot]],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  )(parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, UnassignmentValidationResult] = {
    val fullTree = parsedRequest.fullViewTree

    val reassignmentId: ReassignmentId =
      ReassignmentId(fullTree.sourceSynchronizer, parsedRequest.requestTimestamp)

    for {

      validationResult <- EitherT.right(
        performValidation(
          sourceTopology,
          targetTopology,
          activenessF,
        )(parsedRequest)
      )

      hostedStakeholders <- EitherT.right(
        sourceTopology.unwrap
          .hostedOn(fullTree.stakeholders.all, participantId)
          .map(_.keySet)
      )

      assignmentExclusivity <- targetTopology.traverse { targetTopology =>
        ProcessingSteps
          .getAssignmentExclusivity(targetTopology, fullTree.targetTimeProof.timestamp)
          .leftMap(
            ReassignmentParametersError(
              fullTree.targetSynchronizer.unwrap,
              _,
            ): ReassignmentProcessorError
          )
      }

    } yield UnassignmentValidationResult(
      fullTree = fullTree,
      reassignmentId = reassignmentId,
      hostedStakeholders = hostedStakeholders,
      assignmentExclusivity = assignmentExclusivity,
      validationResult = validationResult,
    )
  }

  private def performValidation(
      sourceTopology: Source[TopologySnapshot],
      targetTopology: Option[Target[TopologySnapshot]],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  )(parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[UnassignmentValidationResult.ValidationResult] = {
    val fullTree = parsedRequest.fullViewTree
    val recipients = parsedRequest.recipients

    val metadataResultET = new ReassignmentValidation(contractAuthenticator).checkMetadata(fullTree)

    for {
      activenessResult <- activenessF
      authenticationErrorO <- AuthenticationValidator.verifyViewSignature(parsedRequest)

      // Now that the contract and metadata are validated, this is safe to use
      expectedStakeholders = fullTree.contracts.stakeholders
      packageIds = fullTree.contracts.packageIds

      submitterCheckResult <-
        ReassignmentValidation
          .checkSubmitter(
            ReassignmentRef(fullTree.contracts.contractIds.toSet),
            topologySnapshot = sourceTopology,
            submitter = fullTree.submitter,
            participantId = fullTree.submitterMetadata.submittingParticipant,
            stakeholders = expectedStakeholders.all,
          )
          .value
          .map(_.swap.toSeq)

      reassigningParticipantCheckResult <- targetTopology match {
        case Some(targetTopology) =>
          new UnassignmentValidationReassigningParticipant(
            sourceTopology,
            targetTopology,
          )(fullTree, recipients)
            .check(expectedStakeholders, packageIds)
            .value
            .map(_.swap.toSeq)
        case None =>
          FutureUnlessShutdown.pure(Seq.empty)
      }

    } yield UnassignmentValidationResult.ValidationResult(
      activenessResult = activenessResult,
      authenticationErrorO = authenticationErrorO,
      metadataResultET = metadataResultET,
      validationErrors = submitterCheckResult ++ reassigningParticipantCheckResult,
    )
  }

}
