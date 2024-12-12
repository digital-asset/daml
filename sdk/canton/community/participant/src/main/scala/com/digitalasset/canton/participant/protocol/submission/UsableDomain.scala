// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.implicits.catsSyntaxSemigroup
import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.daml.lf.transaction.TransactionVersion
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.AbortedDueToShutdownException
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  PackageNotDeclaredCheckOnlyBy,
  PackageNotVettedBy,
  PackageStateError,
  PackageUnknownTo,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.{DamlLfVersionToProtocolVersions, ProtocolVersion}
import com.digitalasset.canton.{LfPackageId, LfPartyId}
import com.digitalasset.daml.lf.transaction.PackageRequirements

import scala.concurrent.{ExecutionContext, Future}

object UsableDomain {

  def check(
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      snapshot: TopologySnapshot,
      requiredPackagesByParty: Map[LfPartyId, PackageRequirements],
      transactionVersion: TransactionVersion,
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, DomainNotUsedReason, Unit] = {

    val packageTopologyRequirementsMet: EitherT[Future, InvalidPackagesStateErrors, Unit] =
      // TODO(#21671): Extract and unit
      resolveParticipantsAndCheckPackageTopologyRequirements(
        domainId,
        snapshot,
        requiredPackagesByParty,
      )
        .failOnShutdownTo(AbortedDueToShutdownException("Usable domain checking"))
    val partiesConnected: EitherT[Future, MissingActiveParticipant, Unit] =
      checkConnectedParties(domainId, snapshot, requiredPackagesByParty.keySet)
    val compatibleProtocolVersion: EitherT[Future, UnsupportedMinimumProtocolVersion, Unit] =
      checkProtocolVersion(domainId, protocolVersion, transactionVersion)

    for {
      _ <- packageTopologyRequirementsMet.leftWiden[DomainNotUsedReason]
      _ <- partiesConnected.leftWiden[DomainNotUsedReason]
      _ <- compatibleProtocolVersion.leftWiden[DomainNotUsedReason]
    } yield ()

  }

  /** Check that every party in `parties` is hosted by an active participant on domain `domainId`
    */
  private def checkConnectedParties(
      domainId: DomainId,
      snapshot: TopologySnapshot,
      parties: Set[LfPartyId],
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, MissingActiveParticipant, Unit] =
    snapshot
      .allHaveActiveParticipants(parties, _.isActive)
      .leftMap(MissingActiveParticipant(domainId, _))

  private def notMeetingPackageTopologyRequirements(snapshot: TopologySnapshot)(
      participantIdAndRequiredPackages: (ParticipantId, PackageRequirements)
  )(implicit ec: ExecutionContext): FutureUnlessShutdown[List[PackageStateError]] = {
    val (participantId, requirements) = participantIdAndRequiredPackages

    val normalizedRequirements = requirements.normalized
    val result = for {
      notCheckOnly <- snapshot.findPackagesOrDependenciesNotDeclaredAsCheckOnly(
        participantId,
        normalizedRequirements.checkOnly,
      )
      unknown <-
        if (notCheckOnly.sizeIs > 0)
          snapshot.findUnvettedPackagesOrDependencies(participantId, notCheckOnly)
        else EitherT.rightT(Set.empty): EitherT[FutureUnlessShutdown, LfPackageId, Set[LfPackageId]]
      notVetted <- snapshot.findUnvettedPackagesOrDependencies(
        participantId,
        normalizedRequirements.vetted,
      )
    } yield (unknown.view.map(
      PackageNotDeclaredCheckOnlyBy(_, participantId)
    ) ++ notVetted.view.map(PackageNotVettedBy(_, participantId))).toList

    result.leftMap(PackageUnknownTo(_, participantId) :: Nil).merge
  }

  private def resolveParticipants(
      snapshot: TopologySnapshot,
      requiredPackagesByParty: Map[LfPartyId, PackageRequirements],
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, Nothing, Map[ParticipantId, PackageRequirements]] =
    requiredPackagesByParty.toList.foldM(Map.empty[ParticipantId, PackageRequirements]) {
      case (acc, (party, packages)) =>
        for {
          // fetch all participants of this party
          participants <- EitherT.right(snapshot.activeParticipantsOf(party))
        } yield {
          // add the required packages for this party to the set of required packages of this participant
          participants.foldLeft(acc) { case (res, (participantId, _)) =>
            res.updated(
              participantId,
              res.getOrElse(participantId, PackageRequirements.empty) |+| packages,
            )
          }
        }
    }

  /** The following is checked:
    *
    * - For every (`party`, `reqs`) in `packageRequirementsByParty`
    *
    * - For every participant `P` hosting `party`
    *
    * - The following package topology requirements are satisfied:
    *   1. All `PackageRequirements.checkOnly` packages are known (i.e. vetted or check-only) by `P` on domain `domainId`
    *   1. All `PackageRequirements.vetted` packages are vetted by `P` on domain `domainId`
    *
    * Note: in order to avoid false errors, it is important that the set of packages needed
    * for the parties hosted locally covers the set of packages needed for all the parties.
    *
    * This is guaranteed in the following situations:
    *
    * - Phase 1:
    * Because the submitting participant hosts one of the authorizers, which sees the whole
    * transaction. Hence, they need all the packages necessary for the transaction.
    *
    * - Phase 3:
    * The participant receives a projection for the parties it hosts. Hence, the packages
    * needed for these parties will be sufficient to re-interpret the whole projection.
    */
  def resolveParticipantsAndCheckPackageTopologyRequirements(
      domainId: DomainId,
      snapshot: TopologySnapshot,
      packageRequirementsByParty: Map[LfPartyId, PackageRequirements],
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, InvalidPackagesStateErrors, Unit] =
    resolveParticipants(snapshot, packageRequirementsByParty)
      .mapK(FutureUnlessShutdown.outcomeK)
      .flatMap(checkPackageTopologyRequirements(domainId, snapshot, _))

  private def checkPackageTopologyRequirements(
      domainId: DomainId,
      snapshot: TopologySnapshot,
      packageRequirements: Map[ParticipantId, PackageRequirements],
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, InvalidPackagesStateErrors, Unit] =
    EitherT(
      packageRequirements.toList
        .parFlatTraverse(notMeetingPackageTopologyRequirements(snapshot))
        .map(NonEmpty.from(_).toLeft(()))
    ).leftMap(unknownTo => InvalidPackagesStateErrors(domainId, unknownTo))

  private def checkProtocolVersion(
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      transactionVersion: TransactionVersion,
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, UnsupportedMinimumProtocolVersion, Unit] = {
    val minimumPVForTransaction =
      DamlLfVersionToProtocolVersions.getMinimumSupportedProtocolVersion(
        transactionVersion
      )

    EitherTUtil.condUnitET(
      protocolVersion >= minimumPVForTransaction,
      UnsupportedMinimumProtocolVersion(
        domainId,
        protocolVersion,
        minimumPVForTransaction,
        transactionVersion,
      ),
    )
  }

  sealed trait DomainNotUsedReason {
    def domainId: DomainId
  }

  final case class MissingActiveParticipant(domainId: DomainId, parties: Set[LfPartyId])
      extends DomainNotUsedReason {
    override def toString: String =
      s"Parties $parties don't have an active participant on domain $domainId"
  }

  final case class InvalidPackagesStateErrors(
      domainId: DomainId,
      packageStateErrors: Seq[PackageStateError],
  ) extends DomainNotUsedReason {
    override def toString: String =
      s"The topology state for some packages does not meet the requirements on domain $domainId: " + packageStateErrors
        .map(_.toString)
        .mkString("[(", "), (", ")]")
  }

  final case class UnsupportedMinimumProtocolVersion(
      domainId: DomainId,
      currentPV: ProtocolVersion,
      requiredPV: ProtocolVersion,
      lfVersion: TransactionVersion,
  ) extends DomainNotUsedReason {

    override def toString: String =
      s"The transaction uses a specific LF version $lfVersion that is supported starting protocol version: $requiredPV. Currently the Domain $domainId is using $currentPV."

  }
}
