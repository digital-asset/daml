// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.{
  PermissionErrors,
  StakeholderHostingErrors,
}
import com.digitalasset.canton.protocol.Stakeholders
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SingletonTraverse.syntax.SingletonTraverseOps
import com.digitalasset.canton.util.{EitherTUtil, EitherUtil, ReassignmentTag, SingletonTraverse}

import scala.concurrent.{ExecutionContext, Future}

private[protocol] class ReassigningParticipantsComputation(
    stakeholders: Stakeholders,
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
    * - one stakeholder is only hosted with observation permission on the source or target domain
    * - one stakeholder does not have enough reassigning participants to meet the threshold defined on both source and target domain
    */
  def compute: EitherT[Future, UnassignmentProcessorError, Set[ParticipantId]] =
    for {
      sourceStakeholdersInfo <- getStakeholdersPartyInfo(sourceTopology)
      targetStakeholdersInfo <- getStakeholdersPartyInfo(targetTopology)

      sourcePermissions = sourceStakeholdersInfo.map(_.fmap(_.participants))
      targetPermissions = targetStakeholdersInfo.map(_.fmap(_.participants))

      _ <- EitherT
        .fromEither[Future](partySubmissionCheck(sourcePermissions, targetPermissions))
        .leftWiden

      confirmingReassigningParticipants <- EitherT.fromEither[Future](
        computeReassigningParticipants(sourcePermissions, targetPermissions)
      )

      missingReassigningParticipantsFor = stakeholders.all.diff(
        confirmingReassigningParticipants.keySet
      )

      _ <- EitherTUtil
        .condUnitET[Future](
          missingReassigningParticipantsFor.isEmpty,
          StakeholderHostingErrors(
            s"The following stakeholders are not hosted on reassigning participants: $missingReassigningParticipantsFor"
          ),
        )
        .leftWiden[UnassignmentProcessorError]

      _ <- EitherT.fromEither[Future](
        validateReassigningParticipantsCount(
          sourceStakeholdersInfo,
          targetStakeholdersInfo,
          confirmingReassigningParticipants,
        )
      )

    } yield confirmingReassigningParticipants.values.flatten.toSet

  /* Checks the following hosting requirements for each party:
   * If the party is hosted on a participant with submission permission,
   * then at least one such participant must also have submission permission
   * for that party on the target domain.
   * This ensures that the party can initiate the assignment if needed and continue to use the contract on the
   * target domain, unless the permissions change in between.
   */
  // TODO(#21795) Check whether we want to keep that requirement
  private def partySubmissionCheck(
      permissionsSource: Source[Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]]],
      permissionsTarget: Target[Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]]],
  ): Either[PermissionErrors, Unit] = {

    def keepParticipantsSubmissionPermission(
        permissions: Map[ParticipantId, ParticipantAttributes]
    ): Set[ParticipantId] =
      permissions.toSeq.collect {
        case (participant, participantPermissions)
            if participantPermissions.permission == ParticipantPermission.Submission =>
          participant
      }.toSet

    permissionsSource.unwrap.toSeq.traverse_ { case (party, sourcePermissions) =>
      val source = keepParticipantsSubmissionPermission(sourcePermissions)
      val target =
        keepParticipantsSubmissionPermission(permissionsTarget.unwrap.getOrElse(party, Map.empty))

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
      permissionsSource: Source[Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]]],
      permissionsTarget: Target[Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]]],
  ): Either[StakeholderHostingErrors, Map[LfPartyId, NonEmpty[Set[ParticipantId]]]] =
    for {
      confirmationSource <- keepConfirmingParticipants(permissionsSource)
      confirmationTarget <- keepConfirmingParticipants(permissionsTarget)

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
      permissionsTagged: ReassignmentTag[Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]]]
  ): Either[StakeholderHostingErrors, Map[LfPartyId, Set[ParticipantId]]] = {
    val permissions = permissionsTagged.unwrap
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
        s"The following stakeholders are not hosted with confirmation rights on ${permissionsTagged.kind} domain: $missingParties"
      ).asLeft
  }

  // Returns the list of participants hosting at least one of the stakeholders.
  // Fails if one stakeholder is unknown.
  private def getStakeholdersPartyInfo[T[X] <: ReassignmentTag[X]: SingletonTraverse](
      topologySnapshot: T[TopologySnapshot]
  ): EitherT[Future, StakeholderHostingErrors, T[Map[LfPartyId, PartyInfo]]] =
    EitherT(
      topologySnapshot
        .traverseSingleton((_, topology) =>
          topology.activeParticipantsOfPartiesWithInfo(stakeholders.all.toSeq).map { permissions =>
            val unknownParties = stakeholders.all.diff(permissions.keySet)
            val partiesWithoutParticipants = permissions.filter { case (_, partyInfo) =>
              partyInfo.participants.isEmpty
            }

            val missingParties = unknownParties.union(partiesWithoutParticipants.keySet)

            if (missingParties.isEmpty)
              permissions.asRight
            else
              StakeholderHostingErrors(
                s"The following parties are not active on the ${topologySnapshot.kind} domain: $missingParties"
              ).asLeft
          }
        )
        .map(_.sequence)
    )

  private def validateReassigningParticipantsCount(
      sourceStakeholdersInfo: Source[Map[LfPartyId, PartyInfo]],
      targetStakeholdersInfo: Target[Map[LfPartyId, PartyInfo]],
      computedReassigningParticipant: Map[LfPartyId, NonEmpty[Set[ParticipantId]]],
  ): Either[UnassignmentProcessorError, Unit] =
    stakeholders.all.toList
      .traverse_ { stakeholder =>
        for {
          sourceThreshold <- sourceStakeholdersInfo.traverse(getThresholdFor(stakeholder, _))
          targetThreshold <- targetStakeholdersInfo.traverse(getThresholdFor(stakeholder, _))

          requiredReassigningParticipants = sourceThreshold.unwrap.max(targetThreshold.unwrap)
          _ <- EitherUtil.condUnitE(
            computedReassigningParticipant
              .get(stakeholder)
              .exists(participants => participants.size >= requiredReassigningParticipants.unwrap),
            StakeholderHostingErrors(
              s"Stakeholder $stakeholder requires at least $requiredReassigningParticipants reassigning participants, but only ${computedReassigningParticipant(stakeholder).size} are available"
            ),
          )

        } yield ()
      }

  private def getThresholdFor(
      party: LfPartyId,
      partyInfo: Map[LfPartyId, PartyInfo],
  ): Either[StakeholderHostingErrors, PositiveInt] =
    partyInfo
      .get(party)
      .map(_.threshold)
      .toRight(
        StakeholderHostingErrors(s"Stakeholder $party not found on target domain")
      )

}
