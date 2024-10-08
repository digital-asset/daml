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
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

// Additional validations for reassigning participants
private[reassignment] sealed abstract case class UnassignmentValidationReassigningParticipant(
    request: FullUnassignmentTree,
    expectedStakeholders: Set[LfPartyId],
    sourceProtocolVersion: Source[ProtocolVersion],
    sourceTopology: Source[TopologySnapshot],
    targetTopology: Target[TopologySnapshot],
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
        targetTopology.unwrap,
        stakeholders.view.map(_ -> Set(templateId.packageId)).toMap,
        targetTopology.unwrap.referenceTime,
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
      sourceProtocolVersion: Source[ProtocolVersion],
      sourceTopology: Source[TopologySnapshot],
      targetTopology: Target[TopologySnapshot],
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
      unassignmentRequestRecipients <- sourceTopology.unwrap
        .activeParticipantsOfAll(expectedStakeholders.toList)
        .mapK(FutureUnlessShutdown.outcomeK)
        .leftMap(inactiveParties =>
          StakeholderHostingErrors(s"The following stakeholders are not active: $inactiveParties")
        )

      reassigningParticipants <- new ReassigningParticipants(
        expectedStakeholders,
        sourceTopology,
        targetTopology,
      ).compute.mapK(FutureUnlessShutdown.outcomeK)
      _ <- validation.checkRecipients(unassignmentRequestRecipients)
      _ <- validation.checkReassigningParticipants(reassigningParticipants)
      _ <- validation.checkVetted(expectedStakeholders, expectedTemplateId)
    } yield ()
  }

}
