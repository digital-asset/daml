// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.store.AcsInspection
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError
import com.digitalasset.canton.topology.{
  DomainId,
  ForceFlag,
  ForceFlags,
  PartyId,
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.ExecutionContext

trait ParticipantTopologyValidation extends NamedLogging {
  def validatePackageVetting(
      currentlyVettedPackages: Set[LfPackageId],
      nextPackageIds: Set[LfPackageId],
      packageDependencyResolver: PackageDependencyResolver,
      acsInspections: () => Map[DomainId, AcsInspection],
      forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    val toBeAdded = nextPackageIds -- currentlyVettedPackages
    val toBeDeleted = currentlyVettedPackages -- nextPackageIds
    for {
      _ <- checkPackageDependencies(
        currentlyVettedPackages,
        toBeAdded,
        packageDependencyResolver,
        forceFlags,
      )
      _ <- toBeDeleted.toList.parTraverse_(packageId => isPackageInUse(packageId, acsInspections))
    } yield ()
  }

  def checkCannotDisablePartyWithActiveContracts(
      party: PartyId,
      forceFlags: ForceFlags,
      acsInspections: () => Map[DomainId, AcsInspection],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    acsInspections().toList
      .parTraverse_ { case (domainId, acsInspection) =>
        EitherT(
          acsInspection
            .hasActiveContracts(party)
            .map {
              case true if !forceFlags.permits(ForceFlag.DisablePartyWithActiveContracts) =>
                Left(
                  TopologyManagerError.ParticipantTopologyManagerError.DisablePartyWithActiveContractsRequiresForce
                    .Reject(party, domainId): TopologyManagerError
                )
              case true =>
                logger.debug(
                  s"Allow to disable $PartyId with active contracts on $domainId because force flag DisablePartyWithActiveContracts is set."
                )
                Right(())
              case false =>
                Right(())
            }
        ).mapK(
          FutureUnlessShutdown.outcomeK
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
        .leftFlatMap[Set[PackageId], TopologyManagerError] { missing =>
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
      packageId: PackageId,
      acsInspections: () => Map[DomainId, AcsInspection],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    acsInspections().toList
      .parTraverse_ { case (domainId, acsInspection) =>
        EitherT(
          acsInspection.activeContractStore
            .packageUsage(packageId, acsInspection.contractStore)
            .map {
              case Some(contractId) =>
                Left(
                  ParticipantTopologyManagerError.PackageIdInUse
                    .Reject(packageId, contractId, domainId): TopologyManagerError
                )
              case None =>
                Right(())
            }
        ).mapK(
          FutureUnlessShutdown.outcomeK
        )
      }
}
