// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.FullUnassignmentTree
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.transfer.ReassignmentProcessingSteps.*
import com.digitalasset.canton.protocol.LfTemplateId
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.version.Transfer.SourceProtocolVersion

import scala.concurrent.ExecutionContext

/** Checks that need to be performed as part of phase 3 of the unassignment request processing
  */
private[transfer] final case class UnassignmentValidation(
    request: FullUnassignmentTree,
    expectedStakeholders: Set[LfPartyId],
    expectedTemplateId: LfTemplateId,
    sourceProtocolVersion: SourceProtocolVersion,
    sourceTopology: TopologySnapshot,
    targetTopology: Option[TopologySnapshot],
    recipients: Recipients,
) {

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

  private def checkParticipants(logger: TracedLogger)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    targetTopology match {
      case Some(targetTopology) =>
        UnassignmentValidationReassigningParticipant(
          request,
          expectedStakeholders,
          expectedTemplateId,
          sourceProtocolVersion,
          sourceTopology,
          targetTopology,
          recipients,
          logger,
        )
      case None =>
        UnassignmentValidationNonReassigningParticipant(request, sourceTopology, logger)
    }

  private def checkTemplateId()(implicit
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

private[transfer] object UnassignmentValidation {

  def apply(
      request: FullUnassignmentTree,
      expectedStakeholders: Set[LfPartyId],
      expectedTemplateId: LfTemplateId,
      sourceProtocolVersion: SourceProtocolVersion,
      sourceTopology: TopologySnapshot,
      targetTopology: Option[TopologySnapshot],
      recipients: Recipients,
      logger: TracedLogger,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {

    val validation = UnassignmentValidation(
      request,
      expectedStakeholders,
      expectedTemplateId,
      sourceProtocolVersion,
      sourceTopology,
      targetTopology,
      recipients,
    )

    for {
      _ <- validation.checkStakeholders
      _ <- validation.checkParticipants(logger)
      _ <- validation.checkTemplateId()
    } yield ()
  }

}
