// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.store.{ActiveContractStore, ContractStore}
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError
import com.digitalasset.canton.topology.{DomainId, ForceFlag, ForceFlags, TopologyManagerError}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.ExecutionContext

trait ParticipantPackageVettingValidation extends NamedLogging {
  def validate(
      currentlyVettedPackages: Set[LfPackageId],
      nextPackageIds: Set[LfPackageId],
      packageDependencyResolver: PackageDependencyResolver,
      contractStores: () => Map[DomainId, (ActiveContractStore, ContractStore)],
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
      _ <- toBeDeleted.toList.parTraverse_(packageId => isPackageInUse(packageId, contractStores))
    } yield ()
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
      contractStores: () => Map[DomainId, (ActiveContractStore, ContractStore)],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    contractStores().toList
      .parTraverse_ { case (domainId, (activeContractStore, contractStore)) =>
        EitherT(
          activeContractStore
            .packageUsage(packageId, contractStore)
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
