// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.Applicative
import cats.data.{EitherT, Validated, ValidatedNec}
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.ExecutionContext

/** Holds information about what (admin) parties and participants need to be involved in
  * performing the unassignment of a certain contract.
  *
  * @param unassigningParticipants All participants hosting at least one stakeholder (i.e., including observers
  *                     not only signatories), regardless of their permission.
  */
// TODO(#21342) Rename and check usefulness. Possibly we can get rid of the validations and this class
private[protocol] final case class AdminPartiesAndParticipants private (
    unassigningParticipants: Set[ParticipantId]
)

private[protocol] object AdminPartiesAndParticipants {

  def apply(
      stakeholders: Set[LfPartyId],
      sourceTopology: TopologySnapshot,
      targetTopology: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, AdminPartiesAndParticipants] =
    for {
      participantsByParty <- PartyParticipantPermissions(
        stakeholders,
        sourceTopology,
        targetTopology,
      )
      _ <- unassignmentParticipants(participantsByParty)
    } yield {

      val participants =
        participantsByParty.perParty.view
          .map(_.source.all)
          .foldLeft(Set.empty[ParticipantId])(_ ++ _)

      AdminPartiesAndParticipants(participants)
    }

  /* Computes the unassignment participants for the reassignments and checks the following hosting requirements for each party:
   * - If the party is hosted on a participant with submission permission,
   *   then at least one such participant must also have submission permission
   *   for that party on the target domain.
   *   This ensures that the party can initiate the assignment if needed and continue to use the contract on the
   *   target domain, unless the permissions change in between.
   * - The party must be hosted on a participant that has confirmation permission on both domains for this party.
   */
  private def unassignmentParticipants(
      permissions: PartyParticipantPermissions
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {

    def validate(sourceTs: CantonTimestamp, targetTs: CantonTimestamp)(
        permission: PartyParticipantPermissions.PerParty
    ): ValidatedNec[String, Unit] = {

      val PartyParticipantPermissions.PerParty(party, source, target) = permission

      def hasSubmissionPermission =
        source.submitters.isEmpty || source.submitters.exists(target.submitters.contains)

      def missingPermissionMessage =
        show"For party $party, no participant with submission permission on source domain (at $sourceTs) has submission permission on target domain (at $targetTs)."

      def missingConfirmationOverlapMessage =
        show"No participant of the party $party has confirmation permission on both domains at respective timestamps $sourceTs and $targetTs."

      val unassignmentParticipants = source.confirmers.intersect(target.confirmers)

      val submissionPermissionCheck =
        Validated.condNec(hasSubmissionPermission, (), missingPermissionMessage)

      val confirmersOverlap =
        Validated.condNec(
          unassignmentParticipants.nonEmpty,
          (),
          missingConfirmationOverlapMessage,
        )

      Applicative[ValidatedNec[String, *]].productR(submissionPermissionCheck)(confirmersOverlap)
    }

    EitherT.fromEither[FutureUnlessShutdown](
      permissions.perParty
        .traverse(validate(permissions.validityAtSource, permissions.validityAtTarget))
        .bimap(UnassignmentProcessorError.fromChain(PermissionErrors), _ => ())
        .toEither
    )
  }
}
