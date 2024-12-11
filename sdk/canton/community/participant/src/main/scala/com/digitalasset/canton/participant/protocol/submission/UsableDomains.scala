// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.PackageUnknownTo
import com.digitalasset.canton.protocol.LfLanguageVersion
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.{
  DamlLfVersionToProtocolVersions,
  HashingSchemeVersion,
  ProtocolVersion,
}
import com.digitalasset.canton.{LfPackageId, LfPartyId}
import com.digitalasset.daml.lf.transaction.TransactionVersion

import scala.concurrent.ExecutionContext

object UsableDomains {

  /** Split the domains in two categories:
    * - Domains that cannot be used
    * - Domain that can be used
    */
  def check(
      domains: List[(DomainId, ProtocolVersion, TopologySnapshot)],
      requiredPackagesPerParty: Map[LfPartyId, Set[LfPackageId]],
      transactionVersion: LfLanguageVersion,
      ledgerTime: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(List[DomainNotUsedReason], List[DomainId])] = domains
    .parTraverse { case (domainId, protocolVersion, snapshot) =>
      UsableDomains
        .check(
          domainId,
          protocolVersion,
          snapshot,
          requiredPackagesPerParty,
          transactionVersion,
          ledgerTime,
          // TODO(i20688): use ISV to select domain
          Option.empty[HashingSchemeVersion],
        )
        .map(_ => domainId)
        .value
    }
    .map(_.separate)

  def check(
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      snapshot: TopologySnapshot,
      requiredPackagesPerParty: Map[LfPartyId, Set[LfPackageId]],
      transactionVersion: LfLanguageVersion,
      ledgerTime: CantonTimestamp,
      interactiveSubmissionVersionO: Option[HashingSchemeVersion],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, DomainNotUsedReason, Unit] = {

    val packageVetted: EitherT[FutureUnlessShutdown, UnknownPackage, Unit] =
      checkPackagesVetted(
        domainId,
        snapshot,
        requiredPackagesPerParty,
        ledgerTime,
      )
    val partiesConnected: EitherT[FutureUnlessShutdown, MissingActiveParticipant, Unit] =
      checkConnectedParties(domainId, snapshot, requiredPackagesPerParty.keySet)
    val compatibleProtocolVersion
        : EitherT[FutureUnlessShutdown, UnsupportedMinimumProtocolVersion, Unit] =
      checkProtocolVersion(domainId, protocolVersion, transactionVersion)
    val compatibleInteractiveSubmissionVersion
        : EitherT[FutureUnlessShutdown, DomainNotUsedReason, Unit] =
      checkInteractiveSubmissionVersion(domainId, interactiveSubmissionVersionO, protocolVersion)
        .leftWiden[DomainNotUsedReason]

    for {
      _ <- packageVetted.leftWiden[DomainNotUsedReason]
      _ <- partiesConnected.leftWiden[DomainNotUsedReason]
      _ <- compatibleProtocolVersion.leftWiden[DomainNotUsedReason]
      _ <- compatibleInteractiveSubmissionVersion
    } yield ()

  }

  private def checkInteractiveSubmissionVersion(
      domainId: DomainId,
      versionO: Option[HashingSchemeVersion],
      protocolVersion: ProtocolVersion,
  )(implicit
      ec: ExecutionContext
  ): EitherT[
    FutureUnlessShutdown,
    UnsupportedMinimumProtocolVersionForInteractiveSubmission,
    Unit,
  ] = versionO
    .map { version =>
      val minProtocolVersion = HashingSchemeVersion.minProtocolVersionForHSV(version)
      EitherT.cond[FutureUnlessShutdown](
        minProtocolVersion.exists(protocolVersion >= _),
        (),
        UnsupportedMinimumProtocolVersionForInteractiveSubmission(
          domainId = domainId,
          currentPV = protocolVersion,
          requiredPV = minProtocolVersion,
          isVersion = version,
        ),
      )
    }
    .getOrElse(EitherT.pure(()))

  /** Check that every party in `parties` is hosted by an active participant on domain `domainId`
    */
  private def checkConnectedParties(
      domainId: DomainId,
      snapshot: TopologySnapshot,
      parties: Set[LfPartyId],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, MissingActiveParticipant, Unit] =
    snapshot
      .allHaveActiveParticipants(parties)
      .leftMap(MissingActiveParticipant(domainId, _))

  private def unknownPackages(snapshot: TopologySnapshot, ledgerTime: CantonTimestamp)(
      participantIdAndRequiredPackages: (ParticipantId, Set[LfPackageId])
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[List[PackageUnknownTo]] = {
    val (participantId, required) = participantIdAndRequiredPackages
    snapshot
      .findUnvettedPackagesOrDependencies(participantId, required, ledgerTime)
      .map(notVetted => notVetted.map(PackageUnknownTo(_, participantId)).toList)
  }

  private def resolveParticipants(
      snapshot: TopologySnapshot,
      requiredPackagesByParty: Map[LfPartyId, Set[LfPackageId]],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, Nothing, Map[ParticipantId, Set[LfPackageId]]] = EitherT.right(
    snapshot.activeParticipantsOfParties(requiredPackagesByParty.keySet.toSeq).map {
      partyToParticipants =>
        requiredPackagesByParty.toList.foldLeft(Map.empty[ParticipantId, Set[LfPackageId]]) {
          case (acc, (party, packages)) =>
            val participants = partyToParticipants.getOrElse(party, Set.empty)
            // add the required packages for this party to the set of required packages of this participant
            participants.foldLeft(acc) { case (res, participantId) =>
              res.updated(participantId, res.getOrElse(participantId, Set()).union(packages))
            }
        }
    }
  )

  /** The following is checked:
    *
    * - For every (`party`, `pkgs`) in `requiredPackagesByParty`
    *
    * - For every participant `P` hosting `party`
    *
    * - All packages `pkgs` are vetted by `P` on domain `domainId`
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
  def checkPackagesVetted(
      domainId: DomainId,
      snapshot: TopologySnapshot,
      requiredPackagesByParty: Map[LfPartyId, Set[LfPackageId]],
      ledgerTime: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, UnknownPackage, Unit] =
    resolveParticipants(snapshot, requiredPackagesByParty)
      .flatMap(
        checkPackagesVetted(domainId, snapshot, ledgerTime, _)
      )

  private def checkPackagesVetted(
      domainId: DomainId,
      snapshot: TopologySnapshot,
      ledgerTime: CantonTimestamp,
      requiredPackages: Map[ParticipantId, Set[LfPackageId]],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, UnknownPackage, Unit] =
    EitherT(
      requiredPackages.toList
        .parFlatTraverse(unknownPackages(snapshot, ledgerTime))
        .map(NonEmpty.from(_).toLeft(()))
    ).leftMap(unknownTo => UnknownPackage(domainId, unknownTo))

  private def checkProtocolVersion(
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      transactionVersion: TransactionVersion,
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, UnsupportedMinimumProtocolVersion, Unit] = {
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

  final case class UnknownPackage(domainId: DomainId, unknownTo: List[PackageUnknownTo])
      extends DomainNotUsedReason {
    override def toString: String =
      (s"Some packages are not known to all informees on domain $domainId" +: unknownTo.map(
        _.toString
      )).mkString(System.lineSeparator())
  }

  final case class UnsupportedMinimumProtocolVersion(
      domainId: DomainId,
      currentPV: ProtocolVersion,
      requiredPV: ProtocolVersion,
      lfVersion: LfLanguageVersion,
  ) extends DomainNotUsedReason {

    override def toString: String =
      s"The transaction uses a specific LF version $lfVersion that is supported starting protocol version: $requiredPV. Currently the Domain $domainId is using $currentPV."

  }

  final case class UnsupportedMinimumProtocolVersionForInteractiveSubmission(
      domainId: DomainId,
      currentPV: ProtocolVersion,
      requiredPV: Option[ProtocolVersion],
      isVersion: HashingSchemeVersion,
  ) extends DomainNotUsedReason {

    override def toString: String =
      s"The transaction was hashed using a version $isVersion that is supported starting protocol version: $requiredPV. Currently the Domain $domainId is using $currentPV."

  }
}
