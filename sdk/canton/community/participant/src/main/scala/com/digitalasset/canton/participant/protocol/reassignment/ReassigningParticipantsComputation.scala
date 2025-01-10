// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.StakeholderHostingErrors
import com.digitalasset.canton.protocol.Stakeholders
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SingletonTraverse.syntax.SingletonTraverseOps
import com.digitalasset.canton.util.{ReassignmentTag, SingletonTraverse}

import scala.concurrent.ExecutionContext

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
    * - one stakeholder is not hosted on some reassigning participant
    * - one signatory does not have enough signatory reassigning participants to meet
    *   the thresholds defined on both source and target synchronizer
    */
  def compute: EitherT[FutureUnlessShutdown, ReassignmentValidationError, Set[ParticipantId]] =
    for {
      sourceStakeholdersInfo <- getStakeholdersPartyInfo(sourceTopology)
      targetStakeholdersInfo <- getStakeholdersPartyInfo(targetTopology)

      reassigningParticipants <- EitherT
        .fromEither[FutureUnlessShutdown](
          computeReassigningParticipants(sourceStakeholdersInfo, targetStakeholdersInfo)
        )
        .leftWiden[ReassignmentValidationError]

      _ <- EitherT
        .fromEither[FutureUnlessShutdown](
          Seq(sourceStakeholdersInfo, targetStakeholdersInfo)
            .traverse_(checkSignatoryReassigningParticipants(_, reassigningParticipants))
        )
        .leftWiden[ReassignmentValidationError]

    } yield reassigningParticipants

  /** Check that all signatories are hosted on sufficiently many signatory reassigning participants.
    */
  private def checkSignatoryReassigningParticipants(
      permissions: ReassignmentTag[Map[LfPartyId, PartyInfo]],
      reassigningParticipants: Set[ParticipantId],
  ): Either[StakeholderHostingErrors, Unit] =
    stakeholders.signatories.toSeq.traverse_ { signatory =>
      for {
        partyInfo <- permissions.unwrap
          .get(signatory)
          .toRight(
            StakeholderHostingErrors(
              s"Signatory $signatory is not hosted on the ${permissions.kind} domain"
            )
          )

        signatoryReassigningParticipants = partyInfo.participants
          .collect {
            case (participantId, attributes) if attributes.canConfirm => participantId
          }
          .toSet
          .intersect(reassigningParticipants)

        _ <- Either.cond(
          signatoryReassigningParticipants.sizeIs >= partyInfo.threshold.unwrap,
          (),
          StakeholderHostingErrors.missingSignatoryReassigningParticipants(
            signatory,
            synchronizer = permissions.kind,
            threshold = partyInfo.threshold,
            signatoryReassigningParticipants = signatoryReassigningParticipants.size,
          ),
        )
      } yield ()
    }

  /** Compute the reassigning participants
    * Fails if one stakeholder is not hosted on any reassigning participant
    */
  private def computeReassigningParticipants(
      permissionsSource: Source[Map[LfPartyId, PartyInfo]],
      permissionsTarget: Target[Map[LfPartyId, PartyInfo]],
  ): Either[StakeholderHostingErrors, Set[ParticipantId]] =
    stakeholders.all.toSeq
      .traverse { stakeholder =>
        for {
          sourceInfo <- permissionsSource.unwrap
            .get(stakeholder)
            .toRight(
              StakeholderHostingErrors(
                s"Stakeholder $stakeholder is not hosted on the source synchronizer"
              )
            )
          targetInfo <- permissionsTarget.unwrap
            .get(stakeholder)
            .toRight(
              StakeholderHostingErrors(
                s"Stakeholder $stakeholder is not hosted on the target synchronizer"
              )
            )

          reassigningParticipants = sourceInfo.participants.keySet.intersect(
            targetInfo.participants.keySet
          )

          _ <- Either.cond(
            reassigningParticipants.nonEmpty,
            (),
            StakeholderHostingErrors(
              s"Stakeholder $stakeholder requires at least one reassigning participant, but none are available"
            ),
          )

        } yield reassigningParticipants
      }
      .map(_.toSet.flatten)

  // Returns the list of participants hosting at least one of the stakeholders.
  // Fails if one stakeholder is unknown.
  private def getStakeholdersPartyInfo[T[X] <: ReassignmentTag[X]: SingletonTraverse](
      topologySnapshot: T[TopologySnapshot]
  ): EitherT[FutureUnlessShutdown, StakeholderHostingErrors, T[Map[LfPartyId, PartyInfo]]] =
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
                s"The following parties are not active on the ${topologySnapshot.kind} synchronizer: $missingParties"
              ).asLeft
          }
        )
        .map(_.sequence)
    )
}
