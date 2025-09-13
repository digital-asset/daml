// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.protocol.reassignment.IncompleteReassignmentData
import com.digitalasset.canton.participant.store.memory.PackageMetadataView
import com.digitalasset.canton.participant.store.{AcsInspection, ReassignmentStore}
import com.digitalasset.canton.platform.apiserver.services.admin.PackageUpgradeValidator
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError
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
import com.digitalasset.daml.lf.language.Ast

import scala.concurrent.ExecutionContext

trait ParticipantTopologyValidation extends NamedLogging {
  def validatePackageVetting(
      currentlyVettedPackages: Set[LfPackageId],
      nextPackageIds: Set[LfPackageId],
      packageMetadataView: Option[PackageMetadataView],
      packageDependencyResolver: PackageDependencyResolver,
      acsInspections: () => Map[SynchronizerId, AcsInspection],
      forceFlags: ForceFlags,
      disableUpgradeValidation: Boolean,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    val toBeAdded = nextPackageIds -- currentlyVettedPackages
    val toBeDeleted = currentlyVettedPackages -- nextPackageIds
    val toBeKept = currentlyVettedPackages.intersect(nextPackageIds)
    for {
      _ <- checkPackageDependencies(
        toBeKept,
        toBeAdded,
        packageDependencyResolver,
        forceFlags,
      )
      _ <- toBeDeleted.toList.parTraverse_(packageId =>
        isPackageInUse(packageId, acsInspections, forceFlags)
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown] {
        if (
          disableUpgradeValidation || forceFlags.permits(ForceFlag.AllowVetIncompatibleUpgrades)
        ) {
          logger.info(
            show"Skipping upgrade validation for newly-added packages $toBeAdded because force flag ${ForceFlag.AllowVetIncompatibleUpgrades.toString} is set"
          )
          Right(())
        } else {
          // packageMetadata can be empty if the vetting happens before the package service is created
          packageMetadataView match {
            case Some(packageMetadataView) =>
              checkUpgrades(
                nextPackageIds,
                toBeAdded,
                packageMetadataView.getSnapshot,
                packageMetadataView.packageUpgradeValidator,
              )
            case None =>
              logger.info(
                show"Skipping upgrade checks on newly-added packages because package metadata is not available: $toBeAdded"
              )
              Right(())
          }
        }
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
                TopologyManagerError.ParticipantTopologyManagerError.InsufficientParticipantPermissionForSignatoryParty
                  .Reject(party, synchronizerId): TopologyManagerError
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
                TopologyManagerError.ParticipantTopologyManagerError.DisablePartyWithActiveContractsRequiresForce
                  .Reject(party, synchronizerId): TopologyManagerError
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
                    TopologyManagerError.ParticipantTopologyManagerError.InsufficientSignatoryAssigningParticipantsForParty
                      .RejectRemovingParty(
                        party,
                        synchronizerId,
                        reassignmentId,
                      ): TopologyManagerError
                  )
                case Some(nextThreshold) if nextThreshold > currentThreshold =>
                  Left(
                    TopologyManagerError.ParticipantTopologyManagerError.InsufficientSignatoryAssigningParticipantsForParty
                      .RejectThresholdIncrease(
                        party,
                        synchronizerId,
                        reassignmentId,
                        nextThreshold,
                        signatoryAssigningParticipants,
                      ): TopologyManagerError
                  )
                case _ =>
                  Left(
                    TopologyManagerError.ParticipantTopologyManagerError.InsufficientSignatoryAssigningParticipantsForParty
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
      currentlyVettedPackages: Set[LfPackageId],
      toBeAdded: Set[LfPackageId],
      packageDependencyResolver: PackageDependencyResolver,
      forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    for {
      dependencies <- packageDependencyResolver
        .packageDependencies(toBeAdded.toList)
        .leftFlatMap[Set[LfPackageId], TopologyManagerError] { missing =>
          if (forceFlags.permits(ForceFlag.AllowUnknownPackage))
            EitherT.rightT(Set.empty)
          else
            EitherT.leftT(
              ParticipantTopologyManagerError.CannotVetDueToMissingPackages
                .Missing(missing): TopologyManagerError
            )
        }

      // check that all dependencies are vetted.
      unvetted = dependencies -- currentlyVettedPackages
      _ <- EitherT
        .cond[FutureUnlessShutdown](
          unvetted.isEmpty || forceFlags.permits(ForceFlag.AllowUnvettedDependencies),
          (),
          ParticipantTopologyManagerError.DependenciesNotVetted
            .Reject(unvetted): TopologyManagerError,
        )
    } yield ()

  private def isPackageInUse(
      packageId: LfPackageId,
      acsInspections: () => Map[SynchronizerId, AcsInspection],
      forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    acsInspections().toList
      .parTraverse_ { case (synchronizerId, acsInspection) =>
        EitherT(
          acsInspection.activeContractStore
            .packageUsage(packageId, acsInspection.contractStore)
            .map {
              case Some(_) if forceFlags.permits(ForceFlag.AllowUnvetPackageWithActiveContracts) =>
                logger.debug(
                  s"Allowing the unvetting of $packageId on $synchronizerId because force flag ${ForceFlag.AllowUnvetPackageWithActiveContracts} is set."
                )
                Either.unit
              case Some(contractId) =>
                Left(
                  ParticipantTopologyManagerError.PackageIdInUse
                    .Reject(packageId, contractId, synchronizerId): TopologyManagerError
                )
              case None =>
                Either.unit
            }
        )
      }

  private def checkUpgrades(
      nextPackageIds: Set[LfPackageId],
      toBeAdded: Set[LfPackageId],
      packageMetadata: PackageMetadata,
      packageUpgradeValidator: PackageUpgradeValidator,
  )(implicit traceContext: TraceContext): Either[TopologyManagerError, Unit] = {
    def getPackageSignature(packageId: LfPackageId): Ast.PackageSignature =
      packageMetadata.packages.getOrElse(
        packageId,
        throw new IllegalStateException(
          s"Missing package-id $packageId in the package metadata view"
        ),
      )

    def isUpgradeable(packageId: LfPackageId): Boolean =
      packageMetadata.packageUpgradabilityMap
        .getOrElse(
          packageId,
          throw new IllegalStateException(
            s"Missing package-id $packageId in the package upgradability map"
          ),
        )

    // We need to check the entire lineage of newly added packages.
    // Removing a package can not lead to incompatible upgrade relationships between the remaining
    // packages in the lineage.
    val affectedPackageNames = toBeAdded.map(getPackageSignature(_).metadata.name)
    val packagesToCheck = nextPackageIds.toList
      .map(packageId => packageId -> getPackageSignature(packageId))
      .filter { case (packageId, pkg) =>
        affectedPackageNames.contains(pkg.metadata.name) && isUpgradeable(packageId)
      }
    packageUpgradeValidator
      .validateUpgrade(packagesToCheck)(LoggingContextWithTrace(loggerFactory))
  }
}
