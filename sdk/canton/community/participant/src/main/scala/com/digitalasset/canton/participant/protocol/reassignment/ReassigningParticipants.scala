// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.{
  PermissionErrors,
  StakeholderHostingErrors,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{EitherTUtil, EitherUtil}

import scala.concurrent.{ExecutionContext, Future}

private[protocol] class ReassigningParticipants(
    stakeholders: Set[LfPartyId],
    sourceTopology: Source[TopologySnapshot],
    targetTopology: Target[TopologySnapshot],
)(implicit
    traceContext: TraceContext,
    ec: ExecutionContext,
) {

  /** Compute the list of reassigning participant.
    *
    * Returns an error if:
    * - one stakeholder is not hosted on a reassigning participant
    */
  def compute: EitherT[Future, UnassignmentProcessorError, Set[ParticipantId]] =
    for {
      permissionsSource <- getStakeholdersPermissions(sourceTopology.unwrap, "source")
      permissionsTarget <- getStakeholdersPermissions(targetTopology.unwrap, "target")

      _ <- EitherT
        .fromEither[Future](partySubmissionCheck(permissionsSource, permissionsTarget))
        .leftWiden

      reassigningParticipantsE = computeReassigningParticipants(
        permissionsSource,
        permissionsTarget,
      )
      reassigningParticipants <- EitherT.fromEither[Future](reassigningParticipantsE)

      missingReassigningParticipantsFor = stakeholders.diff(reassigningParticipants.keySet)
      _ <- EitherTUtil
        .condUnitET[Future](
          missingReassigningParticipantsFor.isEmpty,
          StakeholderHostingErrors(
            s"The following stakeholders are not hosted on reassigning participants: $missingReassigningParticipantsFor"
          ),
        )
        .leftWiden[UnassignmentProcessorError]
    } yield reassigningParticipants.values.flatten.toSet

  /* Checks the following hosting requirements for each party:
   * If the party is hosted on a participant with submission permission,
   * then at least one such participant must also have submission permission
   * for that party on the target domain.
   * This ensures that the party can initiate the assignment if needed and continue to use the contract on the
   * target domain, unless the permissions change in between.
   */
  // TODO(#18531) Check whether we want to keep that requirement
  private def partySubmissionCheck(
      permissionsSource: Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]],
      permissionsTarget: Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]],
  ): Either[PermissionErrors, Unit] = {

    def keepParticipantsSubmissionPermission(
        permissions: Map[ParticipantId, ParticipantAttributes]
    ): Set[ParticipantId] =
      permissions.toSeq.collect {
        case (participant, permissions)
            if permissions.permission == ParticipantPermission.Submission =>
          participant
      }.toSet

    permissionsSource.toSeq.traverse_ { case (party, sourcePermissions) =>
      val source = keepParticipantsSubmissionPermission(sourcePermissions)
      val target =
        keepParticipantsSubmissionPermission(permissionsTarget.getOrElse(party, Map.empty))

      def hasSubmissionPermission = source.isEmpty || source.exists(target.contains)

      EitherUtil.condUnitE(
        hasSubmissionPermission,
        PermissionErrors(
          s"For party $party, no participant with submission permission on source domain has submission permission on target domain."
        ),
      )
    }

  }

  private def computeReassigningParticipants(
      permissionsSource: Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]],
      permissionsTarget: Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]],
  ): Either[StakeholderHostingErrors, Map[LfPartyId, NonEmpty[Set[ParticipantId]]]] =
    for {
      confirmationSource <- keepConfirmingParticipants(permissionsSource, "source")

      confirmationTarget <- keepConfirmingParticipants(permissionsTarget, "target")

      reassigningParticipants = confirmationSource.toList.mapFilter { case (party, participants) =>
        confirmationTarget
          .get(party)
          .map(_.intersect(participants))
          .flatMap(NonEmpty.from)
          .map(party -> _)
      }.toMap
    } yield reassigningParticipants

  // Filter out participants that hosts party with observation rights
  // Fails if one party is not hosted on any participant with at least confirmation rights
  private def keepConfirmingParticipants(
      permissions: Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]],
      kind: String,
  ): Either[StakeholderHostingErrors, Map[LfPartyId, Set[ParticipantId]]] = {

    val confirmingParticipants: Map[LfPartyId, Set[ParticipantId]] = permissions.mapFilter {
      participants =>
        val confirmingParticipants = participants.filter { case (_, permissions) =>
          permissions.permission.canConfirm
        }.keySet

        Option.when(confirmingParticipants.nonEmpty)(confirmingParticipants)
    }

    val missingParties = permissions.keySet.diff(confirmingParticipants.keySet)
    if (missingParties.isEmpty) confirmingParticipants.asRight
    else
      StakeholderHostingErrors(
        s"The following stakeholders are not hosted with confirmation rights on $kind domain: $missingParties"
      ).asLeft
  }

  // Returns the list of participants hosting at least one of the stakeholders.
  // Fails if one stakeholder is unknown.
  private def getStakeholdersPermissions(
      topologySnapshot: TopologySnapshot,
      kind: String,
  ): EitherT[Future, StakeholderHostingErrors, Map[
    LfPartyId,
    Map[ParticipantId, ParticipantAttributes],
  ]] =
    EitherT(
      topologySnapshot.activeParticipantsOfPartiesWithInfo(stakeholders.toSeq).map { permissions =>
        val unknownParties = stakeholders.diff(permissions.keySet)
        val partiesWithoutParticipants = permissions.filter { case (_, partyInfo) =>
          partyInfo.participants.isEmpty
        }

        val missingParties = unknownParties.union(partiesWithoutParticipants.keySet)

        if (missingParties.isEmpty)
          permissions.fmap(_.participants).asRight
        else
          StakeholderHostingErrors(
            s"The following parties are not active on the $kind domain: $missingParties"
          ).asLeft
      }
    )
}
