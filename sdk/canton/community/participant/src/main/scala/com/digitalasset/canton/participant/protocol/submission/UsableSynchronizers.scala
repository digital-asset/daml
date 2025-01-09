// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.protocol.{LfActionNode, LfLanguageVersion, LfVersionedTransaction}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, LfTransactionUtil}
import com.digitalasset.canton.version.{
  DamlLfVersionToProtocolVersions,
  HashingSchemeVersion,
  ProtocolVersion,
}
import com.digitalasset.canton.{LfPackageId, LfPartyId}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.TransactionVersion

import scala.concurrent.ExecutionContext

object UsableSynchronizers {

  /** Split the domains in two categories:
    * - Domains that cannot be used
    * - Domain that can be used
    */
  def check(
      domains: List[(SynchronizerId, ProtocolVersion, TopologySnapshot)],
      transaction: LfVersionedTransaction,
      ledgerTime: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(List[SynchronizerNotUsedReason], List[SynchronizerId])] = domains
    .parTraverse { case (synchronizerId, protocolVersion, snapshot) =>
      UsableSynchronizers
        .check(
          synchronizerId,
          protocolVersion,
          snapshot,
          transaction,
          ledgerTime,
          // TODO(i20688): use ISV to select domain
          Option.empty[HashingSchemeVersion],
        )
        .map(_ => synchronizerId)
        .value
    }
    .map(_.separate)

  def check(
      synchronizerId: SynchronizerId,
      protocolVersion: ProtocolVersion,
      snapshot: TopologySnapshot,
      transaction: LfVersionedTransaction,
      ledgerTime: CantonTimestamp,
      interactiveSubmissionVersionO: Option[HashingSchemeVersion],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, SynchronizerNotUsedReason, Unit] = {

    val requiredPackagesPerParty = Blinding.partyPackages(transaction)
    val transactionVersion = transaction.version

    val packageVetted: EitherT[FutureUnlessShutdown, UnknownPackage, Unit] =
      checkPackagesVetted(
        synchronizerId,
        snapshot,
        requiredPackagesPerParty,
        ledgerTime,
      )
    val partiesConnected: EitherT[FutureUnlessShutdown, MissingActiveParticipant, Unit] =
      checkConnectedParties(synchronizerId, snapshot, requiredPackagesPerParty.keySet)
    val partiesWithConfirmingParticipant
        : EitherT[FutureUnlessShutdown, MissingActiveParticipant, Unit] =
      checkConfirmingParties(synchronizerId, transaction, snapshot)
    val compatibleProtocolVersion
        : EitherT[FutureUnlessShutdown, UnsupportedMinimumProtocolVersion, Unit] =
      checkProtocolVersion(synchronizerId, protocolVersion, transactionVersion)
    val compatibleInteractiveSubmissionVersion
        : EitherT[FutureUnlessShutdown, SynchronizerNotUsedReason, Unit] =
      checkInteractiveSubmissionVersion(
        synchronizerId,
        interactiveSubmissionVersionO,
        protocolVersion,
      )
        .leftWiden[SynchronizerNotUsedReason]

    for {
      _ <- packageVetted.leftWiden[SynchronizerNotUsedReason]
      _ <- partiesConnected.leftWiden[SynchronizerNotUsedReason]
      _ <- partiesWithConfirmingParticipant.leftWiden[SynchronizerNotUsedReason]
      _ <- compatibleProtocolVersion.leftWiden[SynchronizerNotUsedReason]
      _ <- compatibleInteractiveSubmissionVersion
    } yield ()

  }

  private def checkInteractiveSubmissionVersion(
      synchronizerId: SynchronizerId,
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
          synchronizerId = synchronizerId,
          currentPV = protocolVersion,
          requiredPV = minProtocolVersion,
          isVersion = version,
        ),
      )
    }
    .getOrElse(EitherT.pure(()))

  /** Check that every confirming party in the transaction is hosted by an active confirming participant
    * on domain `synchronizerId`.
    */
  private def checkConfirmingParties(
      synchronizerId: SynchronizerId,
      transaction: LfVersionedTransaction,
      snapshot: TopologySnapshot,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, MissingActiveParticipant, Unit] = {

    val actionNodes = transaction.nodes.values.collect { case an: LfActionNode => an }

    val requiredConfirmers = actionNodes.flatMap { node =>
      LfTransactionUtil.signatoriesOrMaintainers(node) | LfTransactionUtil.actingParties(node)
    }.toSet

    snapshot
      .allHaveActiveParticipants(requiredConfirmers, _.canConfirm)
      .leftMap(MissingActiveParticipant(synchronizerId, _))
  }

  /** Check that every party in `parties` is hosted by an active participant on domain `synchronizerId`
    */
  private def checkConnectedParties(
      synchronizerId: SynchronizerId,
      snapshot: TopologySnapshot,
      parties: Set[LfPartyId],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, MissingActiveParticipant, Unit] =
    snapshot
      .allHaveActiveParticipants(parties)
      .leftMap(MissingActiveParticipant(synchronizerId, _))

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
    * - All packages `pkgs` are vetted by `P` on domain `synchronizerId`
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
      synchronizerId: SynchronizerId,
      snapshot: TopologySnapshot,
      requiredPackagesByParty: Map[LfPartyId, Set[LfPackageId]],
      ledgerTime: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, UnknownPackage, Unit] =
    resolveParticipants(snapshot, requiredPackagesByParty)
      .flatMap(
        checkPackagesVetted(synchronizerId, snapshot, ledgerTime, _)
      )

  private def checkPackagesVetted(
      synchronizerId: SynchronizerId,
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
    ).leftMap(unknownTo => UnknownPackage(synchronizerId, unknownTo))

  private def checkProtocolVersion(
      synchronizerId: SynchronizerId,
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
        synchronizerId,
        protocolVersion,
        minimumPVForTransaction,
        transactionVersion,
      ),
    )
  }

  sealed trait SynchronizerNotUsedReason {
    def synchronizerId: SynchronizerId
  }

  final case class MissingActiveParticipant(synchronizerId: SynchronizerId, parties: Set[LfPartyId])
      extends SynchronizerNotUsedReason {
    override def toString: String =
      s"Parties $parties don't have an active participant on domain $synchronizerId"
  }

  final case class UnknownPackage(synchronizerId: SynchronizerId, unknownTo: List[PackageUnknownTo])
      extends SynchronizerNotUsedReason {
    override def toString: String =
      (s"Some packages are not known to all informees on domain $synchronizerId" +: unknownTo.map(
        _.toString
      )).mkString(System.lineSeparator())
  }

  final case class UnsupportedMinimumProtocolVersion(
      synchronizerId: SynchronizerId,
      currentPV: ProtocolVersion,
      requiredPV: ProtocolVersion,
      lfVersion: LfLanguageVersion,
  ) extends SynchronizerNotUsedReason {

    override def toString: String =
      s"The transaction uses a specific LF version $lfVersion that is supported starting protocol version: $requiredPV. Currently the Domain $synchronizerId is using $currentPV."

  }

  final case class UnsupportedMinimumProtocolVersionForInteractiveSubmission(
      synchronizerId: SynchronizerId,
      currentPV: ProtocolVersion,
      requiredPV: Option[ProtocolVersion],
      isVersion: HashingSchemeVersion,
  ) extends SynchronizerNotUsedReason {

    override def toString: String =
      s"The transaction was hashed using a version $isVersion that is supported starting protocol version: $requiredPV. Currently the Domain $synchronizerId is using $currentPV."

  }
}
