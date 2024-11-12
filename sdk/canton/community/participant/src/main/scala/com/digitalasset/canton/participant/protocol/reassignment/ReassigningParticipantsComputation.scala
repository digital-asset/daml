// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.ReassigningParticipants
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.{
  PermissionErrors,
  StakeholderHostingErrors,
}
import com.digitalasset.canton.protocol.Stakeholders
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SingletonTraverse.syntax.SingletonTraverseOps
import com.digitalasset.canton.util.{ReassignmentTag, SingletonTraverse}
import monocle.macros.syntax.lens.*

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
    * - one stakeholder is not hosted on some observing reassigning participant
    * - one signatory is not hosted on some confirming reassigning participant
    * - one signatory does not have enough confirming reassigning participants to meet
    *   the threshold defined on both source and target domain
    */
  def compute: EitherT[Future, UnassignmentProcessorError, ReassigningParticipants] =
    for {
      sourceStakeholdersInfo <- getStakeholdersPartyInfo(sourceTopology)
      targetStakeholdersInfo <- getStakeholdersPartyInfo(targetTopology)

      confirmingReassigningParticipants <- EitherT
        .fromEither[Future](
          computeConfirmingReassigningParticipants(sourceStakeholdersInfo, targetStakeholdersInfo)
        )
        .leftWiden[UnassignmentProcessorError]

      observingReassigningParticipants <- EitherT
        .fromEither[Future](
          computeObservingReassigningParticipants(sourceStakeholdersInfo, targetStakeholdersInfo)
        )
        .leftWiden[UnassignmentProcessorError]

      reassigningParticipants <- EitherT.fromEither[Future](
        ReassigningParticipants
          .create(
            confirming = confirmingReassigningParticipants,
            observing = observingReassigningParticipants,
          )
          .leftMap[UnassignmentProcessorError](PermissionErrors.apply)
      )

    } yield reassigningParticipants

  /** Compute the confirming reassigning participants
    * Fails if one signatory is not hosted on sufficiently many confirming reassigning participants
    */
  private def computeConfirmingReassigningParticipants(
      permissionsSource: Source[Map[LfPartyId, PartyInfo]],
      permissionsTarget: Target[Map[LfPartyId, PartyInfo]],
  ): Either[StakeholderHostingErrors, Set[ParticipantId]] = {
    val confirmationSource = keepConfirmingParticipants(permissionsSource.unwrap)
    val confirmationTarget = keepConfirmingParticipants(permissionsTarget.unwrap)

    stakeholders.signatories.toSeq
      .traverse { signatory =>
        for {
          sourceInfo <- confirmationSource
            .get(signatory)
            .toRight(
              StakeholderHostingErrors(s"Signatory $signatory is not hosted on the source domain")
            )
          targetInfo <- confirmationTarget
            .get(signatory)
            .toRight(
              StakeholderHostingErrors(s"Signatory $signatory is not hosted on the target domain")
            )

          reassigningParticipants = sourceInfo.participants.keySet
            .intersect(targetInfo.participants.keySet)

          requiredReassigningParticipants = sourceInfo.threshold.max(targetInfo.threshold)

          _ <- Either.cond(
            reassigningParticipants.sizeIs >= requiredReassigningParticipants.unwrap,
            (),
            StakeholderHostingErrors(
              s"Signatory $signatory requires at least $requiredReassigningParticipants reassigning participants, but only ${reassigningParticipants.size} are available"
            ),
          )

        } yield reassigningParticipants
      }
      .map(_.toSet.flatten)
  }

  /** Compute the observing reassigning participants
    * Fails if one stakeholder is not hosted on any observing reassigning participant
    */
  private def computeObservingReassigningParticipants(
      permissionsSource: Source[Map[LfPartyId, PartyInfo]],
      permissionsTarget: Target[Map[LfPartyId, PartyInfo]],
  ): Either[StakeholderHostingErrors, Set[ParticipantId]] =
    stakeholders.all.toSeq
      .traverse { stakeholder =>
        for {
          sourceInfo <- permissionsSource.unwrap
            .get(stakeholder)
            .toRight(
              StakeholderHostingErrors(s"Signatory $stakeholder is not hosted on the source domain")
            )
          targetInfo <- permissionsTarget.unwrap
            .get(stakeholder)
            .toRight(
              StakeholderHostingErrors(s"Signatory $stakeholder is not hosted on the target domain")
            )

          observingReassigningParticipants = sourceInfo.participants.keySet
            .intersect(targetInfo.participants.keySet)

          _ <- Either.cond(
            observingReassigningParticipants.nonEmpty,
            (),
            StakeholderHostingErrors(
              s"Stakeholder $stakeholder requires at least one reassigning participants, but only none are available"
            ),
          )

        } yield observingReassigningParticipants
      }
      .map(_.toSet.flatten)

  // Filter out participants that hosts party with observation rights
  private def keepConfirmingParticipants(
      permissions: Map[LfPartyId, PartyInfo]
  ): Map[LfPartyId, PartyInfo] =
    permissions.fmap(
      _.focus(_.participants).modify(_.filter { case (_, permissions) => permissions.canConfirm })
    )

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
}
