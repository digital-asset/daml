// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.FullUnassignmentTree
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.*
import com.digitalasset.canton.protocol.LfTemplateId
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.version.Reassignment.SourceProtocolVersion

import scala.concurrent.ExecutionContext

private[reassignment] sealed abstract case class UnassignmentValidationReassigningParticipant(
    request: FullUnassignmentTree,
    expectedStakeholders: Set[LfPartyId],
    sourceProtocolVersion: SourceProtocolVersion,
    sourceTopology: TopologySnapshot,
    targetTopology: TopologySnapshot,
    recipients: Recipients,
) {

  private def checkAdminParties(
      expectedAdminParties: Set[LfPartyId]
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    condUnitET[FutureUnlessShutdown](
      request.adminParties == expectedAdminParties,
      AdminPartiesMismatch(
        contractId = request.contractId,
        expected = expectedAdminParties,
        declared = request.adminParties,
      ),
    )

  private def checkParticipants(
      expectedParticipants: Set[ParticipantId]
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
    val expectedRecipientsTree = Recipients.ofSet(expectedParticipants)
    condUnitET[FutureUnlessShutdown](
      expectedRecipientsTree.contains(recipients),
      RecipientsMismatch(
        contractId = request.contractId,
        expected = expectedRecipientsTree,
        declared = recipients,
      ),
    )
  }

  private def checkVetted(stakeholders: Set[LfPartyId], templateId: LfTemplateId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    ReassignmentKnownAndVetted(
      stakeholders,
      targetTopology,
      request.contractId,
      templateId.packageId,
      request.targetDomain,
    )
}

private[reassignment] object UnassignmentValidationReassigningParticipant {

  def apply(
      request: FullUnassignmentTree,
      expectedStakeholders: Set[LfPartyId],
      expectedTemplateId: LfTemplateId,
      sourceProtocolVersion: SourceProtocolVersion,
      sourceTopology: TopologySnapshot,
      targetTopology: TopologySnapshot,
      recipients: Recipients,
      logger: TracedLogger,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
    val validation = new UnassignmentValidationReassigningParticipant(
      request,
      expectedStakeholders,
      sourceProtocolVersion,
      sourceTopology,
      targetTopology,
      recipients,
    ) {}
    for {
      adminPartiesAndParticipants <- AdminPartiesAndParticipants(
        request.contractId,
        request.submitter,
        expectedStakeholders,
        sourceTopology,
        targetTopology,
        logger,
      )
      _ <- validation.checkAdminParties(adminPartiesAndParticipants.adminParties)
      _ <- validation.checkParticipants(adminPartiesAndParticipants.participants)
      _ <- validation.checkVetted(expectedStakeholders, expectedTemplateId)
    } yield ()
  }

}
