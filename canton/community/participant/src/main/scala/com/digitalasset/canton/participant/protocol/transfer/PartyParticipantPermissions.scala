// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Disabled,
  Observation,
  Submission,
}

import scala.collection.mutable
import scala.concurrent.ExecutionContext

private final case class PartyParticipantPermissions(
    perParty: List[PartyParticipantPermissions.PerParty],
    validityAtSource: CantonTimestamp,
    validityAtTarget: CantonTimestamp,
)

private object PartyParticipantPermissions {

  final case class PerParty(party: LfPartyId, source: Permissions, target: Permissions)

  final case class Permissions(
      submission: Set[ParticipantId],
      confirmation: Set[ParticipantId],
      other: Set[ParticipantId],
  ) {
    require(
      !submission.exists(confirmation.contains),
      "submission and confirmation permissions must be disjoint.",
    )
    require(
      !submission.exists(other.contains),
      "submission and other permissions must be disjoint.",
    )
    require(
      !confirmation.exists(other.contains),
      "confirmation and other permissions must be disjoint.",
    )

    def submitters: Set[ParticipantId] = submission

    def confirmers: Set[ParticipantId] = submission ++ confirmation

    def all: Set[ParticipantId] = submission ++ confirmation ++ other
  }

  def apply(
      stakeholders: Set[LfPartyId],
      sourceTopology: TopologySnapshot,
      targetTopology: TopologySnapshot,
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, Nothing, PartyParticipantPermissions] =
    EitherT.right(
      stakeholders.toList
        .parTraverse(partyParticipants(sourceTopology, targetTopology))
        .map(
          PartyParticipantPermissions(_, sourceTopology.timestamp, targetTopology.timestamp)
        )
    )

  private def partyParticipants(
      sourceTopology: TopologySnapshot,
      targetTopology: TopologySnapshot,
  )(
      stakeholder: LfPartyId
  )(implicit ec: ExecutionContext): FutureUnlessShutdown[PerParty] = {
    val sourceF = partyParticipants(sourceTopology, stakeholder)
    val targetF = partyParticipants(targetTopology, stakeholder)
    for {
      source <- sourceF
      target <- targetF
    } yield PerParty(stakeholder, source, target)
  }

  private def partyParticipants(topology: TopologySnapshot, party: LfPartyId)(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[Permissions] = {

    val submission = mutable.Set.newBuilder[ParticipantId]
    val confirmation = mutable.Set.newBuilder[ParticipantId]
    val other = mutable.Set.newBuilder[ParticipantId]

    FutureUnlessShutdown.outcomeF(topology.activeParticipantsOf(party).map { partyParticipants =>
      partyParticipants.foreach { case (participantId, attributes) =>
        attributes.permission match {
          case Submission => submission += participantId
          case Confirmation => confirmation += participantId
          case Observation => other += participantId
          case Disabled =>
            throw new IllegalStateException(
              s"activeParticipantsOf($party) returned a disabled participant $participantId"
            )
        }
      }
      Permissions(
        submission.result().toSet,
        confirmation.result().toSet,
        other.result().toSet,
      )
    })

  }
}
