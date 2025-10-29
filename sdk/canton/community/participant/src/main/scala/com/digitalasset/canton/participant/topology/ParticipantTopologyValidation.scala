// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLogging}
import com.digitalasset.canton.participant.protocol.reassignment.IncompleteReassignmentData
import com.digitalasset.canton.participant.store.memory.PackageMetadataView
import com.digitalasset.canton.participant.store.{AcsInspection, ReassignmentStore}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError.*
import com.digitalasset.canton.topology.transaction.HostingParticipant
import com.digitalasset.canton.topology.{
  ForceFlag,
  ForceFlags,
  PartyId,
  SynchronizerId,
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.ExecutionContext

trait ParticipantTopologyValidation extends NamedLogging {
  def validatePackageVetting(
      currentlyVettedPackages: Set[LfPackageId],
      nextPackageIds: Set[LfPackageId],
      packageMetadataView: PackageMetadataView,
      dryRunSnapshot: Option[PackageMetadata],
      forceFlags: ForceFlags,
      disableUpgradeValidation: Boolean,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    val toBeAdded = nextPackageIds -- currentlyVettedPackages
    val toBeDeleted = currentlyVettedPackages -- nextPackageIds
    val packageMetadataSnapshot = dryRunSnapshot.getOrElse(packageMetadataView.getSnapshot)
    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        checkPackageDependencies(
          nextPackageIds,
          toBeAdded,
          toBeDeleted,
          packageMetadataSnapshot,
          forceFlags,
        )
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown] {
        if (
          disableUpgradeValidation || forceFlags.permits(ForceFlag.AllowVetIncompatibleUpgrades)
        ) {
          logger.info(
            show"Skipping upgrade validation for newly-added packages $toBeAdded because force flag ${ForceFlag.AllowVetIncompatibleUpgrades.toString} is set"
          )
          Right(())
        } else
          packageMetadataView.packageUpgradeValidator.validateUpgrade(
            toBeAdded,
            nextPackageIds,
            packageMetadataSnapshot.packages,
          )(LoggingContextWithTrace(loggerFactory))
      }
    } yield ()
  }

  def checkInsufficientParticipantPermissionForSignatoryParty(
      party: PartyId,
      forceFlags: ForceFlags,
      acsInspections: () => Map[SynchronizerId, AcsInspection],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    MonadUtil.sequentialTraverse_(acsInspections().toList) { case (synchronizerId, acsInspection) =>
      EitherT(
        acsInspection
          .isSignatoryOnActiveContracts(party)
          .map {
            case true
                if !forceFlags
                  .permits(ForceFlag.AllowInsufficientParticipantPermissionForSignatoryParty) =>
              Left(
                InsufficientParticipantPermissionForSignatoryParty.Reject(party, synchronizerId)
              )
            case true =>
              logger.info(
                s"Allow changing the permission of $party to a pure observer with active contracts on $synchronizerId, " +
                  s"where it is a signatory, because the force flag ${ForceFlag.AllowInsufficientParticipantPermissionForSignatoryParty} is set."
              )
              Either.unit
            case false =>
              Either.unit
          }
      )
    }

  def checkCannotDisablePartyWithActiveContracts(
      party: PartyId,
      forceFlags: ForceFlags,
      acsInspections: () => Map[SynchronizerId, AcsInspection],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    MonadUtil.sequentialTraverse_(acsInspections().toList) { case (synchronizerId, acsInspection) =>
      EitherT(
        acsInspection
          .hasActiveContracts(party)
          .map {
            case true if !forceFlags.permits(ForceFlag.DisablePartyWithActiveContracts) =>
              Left(
                DisablePartyWithActiveContractsRequiresForce.Reject(party, synchronizerId)
              )
            case true =>
              logger.debug(
                s"Allow to disable $party with active contracts on $synchronizerId because force flag ${ForceFlag.DisablePartyWithActiveContracts} is set."
              )
              Either.unit
            case false =>
              Either.unit
          }
      )
    }

  /** Checks that changing the party to participant mapping does not result in stuck assignments.
    * The check involves identifying all incomplete reassignments for the party and verifying that
    * there will still be enough signatory-assigning participants after the mapping change.
    */
  def checkInsufficientSignatoryAssigningParticipantsForParty(
      party: PartyId,
      currentThreshold: PositiveInt,
      nextThresholdO: Option[PositiveInt],
      nextHostingParticipants: Seq[HostingParticipant],
      forceFlags: ForceFlags,
      reassignmentStores: () => Map[SynchronizerId, ReassignmentStore],
      ledgerEnd: () => FutureUnlessShutdown[Option[ParameterStorageBackend.LedgerEnd]],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    MonadUtil.sequentialTraverse_(reassignmentStores().toList) {
      case (synchronizerId, reassignmentStore) =>
        EitherT(
          for {
            ledgerEnd <- ledgerEnd()
            incompleteReassignments <- reassignmentStore.findIncomplete(
              sourceSynchronizer = None,
              validAt = ledgerEnd.map(_.lastOffset).getOrElse(Offset.firstOffset),
              stakeholders = NonEmpty.from(Set(party.toLf)),
              limit = NonNegativeInt.maxValue,
            )
            reassignmentIdToReassigningParticipants = incompleteReassignments.collect {
              case IncompleteReassignmentData(
                    reassignmentId,
                    Some(reassigningParticipants),
                    _,
                    _,
                  ) =>
                reassignmentId -> reassigningParticipants
            }.toMap

            nextConfirmingParticipants = nextHostingParticipants
              .filter(_.canConfirm)
              .map(_.participantId)

            // find the first reassignment that has less reassigning participants than the threshold
            // if the next threshold is not defined, it means we are deactivating the party
            stuckAssignmentO = nextThresholdO
              .map { nextThreshold =>
                reassignmentIdToReassigningParticipants.view
                  .mapValues(_.intersect(nextConfirmingParticipants.toSet))
                  .find(
                    _._2.sizeIs < nextThreshold.value
                  )
              }
              .getOrElse(reassignmentIdToReassigningParticipants.headOption)

          } yield stuckAssignmentO match {
            case Some((reassignmentId, signatoryAssigningParticipants))
                if !forceFlags
                  .permits(ForceFlag.AllowInsufficientSignatoryAssigningParticipantsForParty) =>
              nextThresholdO match {
                case None =>
                  Left(
                    InsufficientSignatoryAssigningParticipantsForParty.RejectRemovingParty(
                      party,
                      synchronizerId,
                      reassignmentId,
                    ): TopologyManagerError
                  )
                case Some(nextThreshold) if nextThreshold > currentThreshold =>
                  Left(
                    InsufficientSignatoryAssigningParticipantsForParty.RejectThresholdIncrease(
                      party,
                      synchronizerId,
                      reassignmentId,
                      nextThreshold,
                      signatoryAssigningParticipants,
                    ): TopologyManagerError
                  )
                case _ =>
                  Left(
                    InsufficientSignatoryAssigningParticipantsForParty
                      .RejectNotEnoughSignatoryAssigningParticipants(
                        party,
                        synchronizerId,
                        reassignmentId,
                        currentThreshold,
                        signatoryAssigningParticipants,
                      ): TopologyManagerError
                  )
              }
            case Some((reassignmentId, _)) =>
              logger.debug(
                s"Allow to change party to participant mapping for $party with incomplete reassignments, such as $reassignmentId, on $synchronizerId because force flag ${ForceFlag.DisablePartyWithActiveContracts} is set."
              )
              Either.unit
            case None =>
              Either.unit
          }
        )
    }

  private def checkPackageDependencies(
      vettedPackagesTarget: Set[LfPackageId],
      toBeAdded: Set[LfPackageId],
      toBeRemoved: Set[LfPackageId],
      packageMetadataSnapshot: PackageMetadata,
      forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): Either[TopologyManagerError, Unit] = {
    def getDependencies(packageId: LfPackageId): Set[LfPackageId] =
      packageMetadataSnapshot.packages.get(packageId) match {
        case Some(pkg) => pkg.directDeps
        case None =>
          logger.warn(
            s"Package dependency checks will be ignored for $packageId as it could not be found in the package metadata. " +
              s"This can happen if the package was previously vetted with ${ForceFlag.AllowUnknownPackage}. " +
              s"If this was not the case, the participant reached an inconsistent state on the package vetting state and should be investigated."
          )
          Set.empty
      }
    val (knownToBeAdded, unknownToBeAdded) =
      toBeAdded.partition(packageMetadataSnapshot.packages.contains)
    val dependenciesOfAdded = packageMetadataSnapshot.allDependenciesRecursively(knownToBeAdded)
    val removedDeps =
      if (toBeRemoved.nonEmpty) vettedPackagesTarget.flatMap(getDependencies).intersect(toBeRemoved)
      else Set.empty
    val unvettedDeps = (dependenciesOfAdded -- vettedPackagesTarget) ++ removedDeps
    if (unknownToBeAdded.nonEmpty && !forceFlags.permits(ForceFlag.AllowUnknownPackage))
      Left(CannotVetDueToMissingPackages.Missing(unknownToBeAdded))
    else if (unvettedDeps.nonEmpty && !forceFlags.permits(ForceFlag.AllowUnvettedDependencies))
      Left(DependenciesNotVetted.Reject(unvettedDeps))
    else Right(())
  }
}
