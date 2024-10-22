// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import com.digitalasset.canton.data.FullUnassignmentTree
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.ReassignmentSubmissionValidation
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.protocol.{LfTemplateId, Stakeholders}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

/** Checks that need to be performed as part of phase 3 of the unassignment request processing
  */
private[reassignment] final case class UnassignmentValidation(
    expectedStakeholders: Stakeholders,
    expectedTemplateId: LfTemplateId,
    sourceProtocolVersion: Source[ProtocolVersion],
    sourceTopology: Source[TopologySnapshot],
    // Defined if and only if the participant is reassigning
    targetTopology: Option[Target[TopologySnapshot]],
    recipients: Recipients,
)(request: FullUnassignmentTree) {

  private def checkStakeholders(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    condUnitET(
      request.stakeholders == expectedStakeholders,
      StakeholdersMismatch(
        None,
        declaredViewStakeholders = request.stakeholders,
        declaredContractStakeholders = None,
        expectedStakeholders = Right(expectedStakeholders),
      ),
    )

  private def checkParticipants(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    targetTopology match {
      case Some(targetTopology) =>
        UnassignmentValidationReassigningParticipant(
          expectedStakeholders = expectedStakeholders,
          expectedTemplateId,
          sourceProtocolVersion,
          sourceTopology,
          targetTopology,
          recipients,
        )(request)
      case None => EitherT.pure(())
    }

  private def checkTemplateId(implicit
      executionContext: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    EitherT.cond[FutureUnlessShutdown](
      expectedTemplateId == request.templateId,
      (),
      TemplateIdMismatch(
        declaredTemplateId = request.templateId,
        expectedTemplateId = expectedTemplateId,
      ),
    )
}

private[reassignment] object UnassignmentValidation {
  def perform(
      expectedStakeholders: Stakeholders,
      expectedTemplateId: LfTemplateId,
      sourceProtocolVersion: Source[ProtocolVersion],
      sourceTopology: Source[TopologySnapshot],
      targetTopology: Option[Target[TopologySnapshot]],
      recipients: Recipients,
  )(request: FullUnassignmentTree)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {

    val validation = UnassignmentValidation(
      expectedStakeholders = expectedStakeholders,
      expectedTemplateId,
      sourceProtocolVersion,
      sourceTopology,
      targetTopology,
      recipients,
    )(request)

    for {
      _ <- validation.checkStakeholders
      _ <- ReassignmentSubmissionValidation.unassignment(
        contractId = request.contractId,
        topologySnapshot = sourceTopology,
        submitter = request.submitter,
        participantId = request.submitterMetadata.submittingParticipant,
        stakeholders = expectedStakeholders.all,
      )
      _ <- validation.checkParticipants
      _ <- validation.checkTemplateId
    } yield ()
  }

}
