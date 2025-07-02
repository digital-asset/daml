// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.digitalasset.canton.data.{FullUnassignmentTree, ReassignmentRef}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.ReassigningParticipantsMismatch
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationError.PackageIdUnknownOrUnvetted
import com.digitalasset.canton.participant.protocol.submission.UsableSynchronizers
import com.digitalasset.canton.protocol.Stakeholders
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{LfPackageId, LfPartyId}

import scala.concurrent.ExecutionContext

// Additional validations for reassigning participants
private[reassignment] class UnassignmentValidationReassigningParticipant(
    sourceTopology: Source[TopologySnapshot],
    targetTopology: Target[TopologySnapshot],
)(request: FullUnassignmentTree) {

  /** check that:
    *   - all stakeholders are hosted on active participants
    *   - the recipients from the request match the computed recipients
    *   - the reassigning participants from the request match the computed reassigning participants
    *   - the package of the template is vetted
    */
  def check(expectedStakeholders: Stakeholders, packageIds: Set[LfPackageId])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] =
    for {
      reassigningParticipants <- new ReassigningParticipantsComputation(
        stakeholders = expectedStakeholders,
        sourceTopology,
        targetTopology,
      ).compute
      _ <- checkReassigningParticipants(reassigningParticipants)
      _ <- checkVetted(expectedStakeholders.all, packageIds)
    } yield ()

  private def checkReassigningParticipants(
      expectedReassigningParticipants: Set[ParticipantId]
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] =
    condUnitET[FutureUnlessShutdown](
      request.reassigningParticipants == expectedReassigningParticipants,
      ReassigningParticipantsMismatch(
        reassignmentRef = ReassignmentRef.ContractIdRef(request.contracts.contractIds.toSet),
        expected = expectedReassigningParticipants,
        declared = request.reassigningParticipants,
      ),
    )

  private def checkVetted(stakeholders: Set[LfPartyId], packageIds: Set[LfPackageId])(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] =
    UsableSynchronizers
      .checkPackagesVetted(
        request.targetSynchronizer.unwrap,
        targetTopology.unwrap,
        stakeholders.view.map(_ -> packageIds).toMap,
        targetTopology.unwrap.referenceTime,
      )
      .leftMap(unknownPackage =>
        PackageIdUnknownOrUnvetted(request.contracts.contractIds.toSet, unknownPackage.unknownTo)
      )
      .leftWiden[ReassignmentValidationError]
}
