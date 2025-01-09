// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{FullUnassignmentTree, ReassignmentRef}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.{
  ReassigningParticipantsMismatch,
  StakeholderHostingErrors,
}
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationError.{
  PackageIdUnknownOrUnvetted,
  RecipientsMismatch,
}
import com.digitalasset.canton.participant.protocol.submission.UsableSynchronizers
import com.digitalasset.canton.protocol.{LfTemplateId, Stakeholders}
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.ExecutionContext

// Additional validations for reassigning participants
private[reassignment] class UnassignmentValidationReassigningParticipant(
    sourceTopology: Source[TopologySnapshot],
    targetTopology: Target[TopologySnapshot],
)(request: FullUnassignmentTree, recipients: Recipients) {

  /** check that:
    * - all stakeholders are hosted on active participants
    * - the recipients from the request match the computed recipients
    * - the reassigning participants from the request match the computed reassigning participants
    * - the package of the template is vetted
    */
  def check(expectedStakeholders: Stakeholders, expectedTemplateId: LfTemplateId)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] =
    for {
      unassignmentRequestRecipients <- sourceTopology.unwrap
        .activeParticipantsOfAll(expectedStakeholders.all.toList)
        .leftMap(inactiveParties =>
          StakeholderHostingErrors(s"The following stakeholders are not active: $inactiveParties")
        )

      reassigningParticipants <- new ReassigningParticipantsComputation(
        stakeholders = expectedStakeholders,
        sourceTopology,
        targetTopology,
      ).compute
      _ <- checkRecipients(unassignmentRequestRecipients)
      _ <- checkReassigningParticipants(reassigningParticipants)
      _ <- checkVetted(expectedStakeholders.all, expectedTemplateId)
    } yield ()

  private def checkReassigningParticipants(
      expectedReassigningParticipants: Set[ParticipantId]
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] =
    condUnitET[FutureUnlessShutdown](
      request.reassigningParticipants == expectedReassigningParticipants,
      ReassigningParticipantsMismatch(
        reassignmentRef = ReassignmentRef(request.contractId),
        expected = expectedReassigningParticipants,
        declared = request.reassigningParticipants,
      ),
    )

  private def checkRecipients(
      expectedRecipients: Set[ParticipantId]
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] = {
    val expectedRecipientsTree = Recipients.ofSet(expectedRecipients)
    condUnitET[FutureUnlessShutdown](
      // TODO(i12926): Is it stable under recipients projections and therefore will it lead to diverging outcomes
      // on different participants for maliciously crafted requests.
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
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] =
    UsableSynchronizers
      .checkPackagesVetted(
        request.targetSynchronizer.unwrap,
        targetTopology.unwrap,
        stakeholders.view.map(_ -> Set(templateId.packageId)).toMap,
        targetTopology.unwrap.referenceTime,
      )
      .leftMap(unknownPackage =>
        PackageIdUnknownOrUnvetted(request.contractId, unknownPackage.unknownTo)
      )
      .leftWiden[ReassignmentValidationError]
}
