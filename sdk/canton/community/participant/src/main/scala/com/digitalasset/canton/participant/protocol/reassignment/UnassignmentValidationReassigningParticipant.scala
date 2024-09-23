// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.FullUnassignmentTree
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.*
import com.digitalasset.canton.participant.protocol.submission.UsableDomain
import com.digitalasset.canton.protocol.LfTemplateId
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.version.Reassignment.SourceProtocolVersion

import scala.concurrent.ExecutionContext

// Additional validations for reassigning participants
private[reassignment] sealed abstract case class UnassignmentValidationReassigningParticipant(
    request: FullUnassignmentTree,
    expectedStakeholders: Set[LfPartyId],
    sourceProtocolVersion: SourceProtocolVersion,
    sourceTopology: TopologySnapshot,
    targetTopology: TopologySnapshot,
    recipients: Recipients,
) {
  private def checkReassigningParticipants(
      expectedReassigningParticipants: Set[ParticipantId]
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    condUnitET[FutureUnlessShutdown](
      request.reassigningParticipants == expectedReassigningParticipants,
      ReassigningParticipantsMismatch(
        contractId = request.contractId,
        expected = expectedReassigningParticipants,
        declared = request.reassigningParticipants,
      ),
    )

  private def checkRecipients(
      expectedRecipients: Set[ParticipantId]
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
    val expectedRecipientsTree = Recipients.ofSet(expectedRecipients)
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
    UsableDomain
      .checkPackagesVetted(
        request.targetDomain.unwrap,
        targetTopology,
        stakeholders.view.map(_ -> Set(templateId.packageId)).toMap,
        targetTopology.referenceTime,
      )
      .leftMap(unknownPackage =>
        UnassignmentProcessorError
          .PackageIdUnknownOrUnvetted(request.contractId, unknownPackage.unknownTo)
      )
      .leftWiden[ReassignmentProcessorError]
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
        expectedStakeholders,
        sourceTopology,
        targetTopology,
      )

      reassigningParticipants <- ReassigningParticipants
        .compute(
          expectedStakeholders,
          sourceTopology,
          targetTopology,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- validation.checkRecipients(adminPartiesAndParticipants.unassigningParticipants)
      _ <- validation.checkReassigningParticipants(reassigningParticipants)
      _ <- validation.checkVetted(expectedStakeholders, expectedTemplateId)
    } yield ()
  }

}
