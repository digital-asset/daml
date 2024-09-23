// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.StakeholderHostingErrors
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.{ExecutionContext, Future}

private[reassignment] object ReassigningParticipants {

  /** Compute the list of reassigning participant.
    *
    * Returns an error if:
    * - one stakeholder is not hosted on a reassigning participant
    */
  def compute(
      stakeholders: Set[LfPartyId],
      sourceTopology: TopologySnapshot,
      targetTopology: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, StakeholderHostingErrors, Set[ParticipantId]] = {

    // Returns the list of participants hosting at least one of the stakeholders.
    // Fails if one stakeholder is unknown.
    def getStakeholdersPermissions(topologySnapshot: TopologySnapshot, kind: String) =
      EitherT(topologySnapshot.activeParticipantsOfPartiesWithInfo(stakeholders.toSeq).map {
        permissions =>
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
      })

    // Filter out participants that hosts party with observation rights
    // Fails if one party is not hosted on any participant with at least confirmation rights
    def keepConfirmingParticipants(
        permissions: Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]],
        kind: String,
    ) = {

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

    for {
      permissionsSource <- getStakeholdersPermissions(sourceTopology, "source")
      permissionsTarget <- getStakeholdersPermissions(targetTopology, "target")

      confirmationSource <- EitherT.fromEither[Future](
        keepConfirmingParticipants(permissionsSource, "source")
      )
      confirmationTarget <- EitherT.fromEither[Future](
        keepConfirmingParticipants(permissionsTarget, "target")
      )

      reassigningParticipants = confirmationSource.toList.mapFilter { case (party, participants) =>
        confirmationTarget
          .get(party)
          .map(_.intersect(participants))
          .flatMap(NonEmpty.from)
          .map(party -> _)
      }.toMap

      missingReassigningParticipantsFor = stakeholders.diff(reassigningParticipants.keySet)
      _ <- EitherTUtil.condUnitET[Future](
        missingReassigningParticipantsFor.isEmpty,
        StakeholderHostingErrors(
          s"The following stakeholders are not hosted on reassigning participants: $missingReassigningParticipantsFor"
        ),
      )
    } yield reassigningParticipants.values.flatten.toSet
  }
}
