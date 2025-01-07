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
import com.digitalasset.canton.participant.protocol.{EngineController, ProcessingSteps}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.{ReassignmentId, Stakeholders}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.ExecutionContext

private[reassignment] class UnassignmentValidation(
    participantId: ParticipantId,
    engine: DAMLe,
) {

  /** @param targetTopology Defined if and only if the participant is reassigning
    */
  def perform(
      sourceTopology: Source[TopologySnapshot],
      targetTopology: Option[Target[TopologySnapshot]],
      activenessF: FutureUnlessShutdown[ActivenessResult],
      engineController: EngineController,
  )(parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, UnassignmentValidationResult] = {
    val fullTree = parsedRequest.fullViewTree

    val reassignmentId: ReassignmentId =
      ReassignmentId(fullTree.sourceSynchronizer, parsedRequest.requestTimestamp)
    val contract = fullTree.contract

    for {

      validationResult <- EitherT.right(
        performValidation(
          sourceTopology,
          targetTopology,
          activenessF,
          engineController,
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
      rootHash = fullTree.rootHash,
      contractId = fullTree.contractId,
      reassignmentCounter = fullTree.reassignmentCounter,
      templateId = contract.rawContractInstance.contractInstance.unversioned.template,
      packageName = contract.rawContractInstance.contractInstance.unversioned.packageName,
      submitterMetadata = fullTree.submitterMetadata,
      reassignmentId = reassignmentId,
      targetDomain = fullTree.targetSynchronizer,
      stakeholders = fullTree.stakeholders.all,
      targetTimeProof = fullTree.targetTimeProof,
      hostedStakeholders = hostedStakeholders,
      assignmentExclusivity = assignmentExclusivity,
      validationResult = validationResult,
    )
  }

  private def performValidation(
      sourceTopology: Source[TopologySnapshot],
      targetTopology: Option[Target[TopologySnapshot]],
      activenessF: FutureUnlessShutdown[ActivenessResult],
      engineController: EngineController,
  )(parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[UnassignmentValidationResult.ValidationResult] = {
    val fullTree = parsedRequest.fullViewTree
    val recipients = parsedRequest.recipients

    // check asynchronously so that we can complete the pending request
    // in the Phase37Synchronizer without waiting for it, thereby allowing us to concurrently receive a
    // mediator verdict.
    val metadataResultET = new ReassignmentValidation(engine)
      .checkMetadata(fullTree, () => engineController.abortStatus)
      .mapK(FutureUnlessShutdown.outcomeK)

    for {
      activenessResult <- activenessF
      authenticationErrorO <- AuthenticationValidator.verifyViewSignature(parsedRequest)

      // Now that the contract and metadata are validated, this is safe to use
      expectedStakeholders = Stakeholders(fullTree.contract.metadata)
      expectedTemplateId =
        fullTree.contract.rawContractInstance.contractInstance.unversioned.template

      submitterCheckResult <-
        ReassignmentValidation
          .checkSubmitter(
            ReassignmentRef(fullTree.contractId),
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
            .check(expectedStakeholders, expectedTemplateId)
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
