// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.implicits.toBifunctorOps
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageMissingDependencies
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.{
  DarUnvettingError,
  PackageInUse,
}
import com.digitalasset.canton.participant.admin.PackageService.DarDescriptor
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.participant.topology.{
  ParticipantTopologyManager,
  ParticipantTopologyManagerError,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

trait PackageOps extends NamedLogging {
  def isPackageVetted(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, CantonError, Boolean]

  def checkPackageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageInUse, Unit]

  def vetPackages(
      packages: Seq[PackageId],
      synchronize: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]

  def revokeVettingForPackages(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: DarDescriptor,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Unit]
}

abstract class PackageOpsCommon(
    participantId: ParticipantId,
    headAuthorizedTopologySnapshot: TopologySnapshot,
    stateManager: SyncDomainPersistentStateManager,
)(implicit
    val ec: ExecutionContext
) extends PackageOps {
  override def checkPackageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageInUse, Unit] =
    stateManager.getAll.toList
      .sortBy(_._1.toProtoPrimitive) // Sort to keep tests deterministic
      .parTraverse_ { case (_, state) =>
        EitherT(
          state.activeContractStore
            .packageUsage(packageId, state.contractStore)
            .map(opt =>
              opt.fold[Either[PackageInUse, Unit]](Right(()))(contractId =>
                Left(new PackageInUse(packageId, contractId, state.domainId.domainId))
              )
            )
        )
      }

  override def isPackageVetted(
      packageId: PackageId
  )(implicit tc: TraceContext): EitherT[Future, CantonError, Boolean] = {
    // Use the aliasManager to query all domains, even those that are currently disconnected
    val snapshotsForDomains: List[TopologySnapshot] =
      stateManager.getAll.view.keys
        .map(stateManager.topologyFactoryFor)
        .flatMap(_.map(_.createHeadTopologySnapshot()))
        .toList

    val packageIsVettedOn = (headAuthorizedTopologySnapshot :: snapshotsForDomains)
      .parTraverse { snapshot =>
        snapshot
          .findUnvettedPackagesOrDependencies(participantId, Set(packageId))
          .map { pkgId =>
            val isVetted = pkgId.isEmpty
            isVetted
          }
      }

    packageIsVettedOn.bimap(PackageMissingDependencies.Reject(packageId, _), _.contains(true))
  }
}

class PackageOpsImpl(
    val participantId: ParticipantId,
    val headAuthorizedTopologySnapshot: TopologySnapshot,
    stateManager: SyncDomainPersistentStateManager,
    topologyManager: ParticipantTopologyManager,
    protocolVersion: ProtocolVersion,
    val loggerFactory: NamedLoggerFactory,
)(implicit override val ec: ExecutionContext)
    extends PackageOpsCommon(participantId, headAuthorizedTopologySnapshot, stateManager) {

  override def vetPackages(
      packages: Seq[PackageId],
      synchronize: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
    val packageSet = packages.toSet

    for {
      unvettedPackages <- EitherT
        .right(topologyManager.unvettedPackages(participantId, packageSet))
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <-
        if (unvettedPackages.isEmpty) {
          logger.debug(show"The following packages are already vetted: $packages")
          EitherT.rightT[FutureUnlessShutdown, ParticipantTopologyManagerError](())
        } else {
          topologyManager.authorize(
            TopologyStateUpdate.createAdd(VettedPackages(participantId, packages), protocolVersion),
            None,
            protocolVersion,
            force = false,
          )
        }

      appeared <-
        if (synchronize)
          topologyManager.waitForPackagesBeingVetted(packageSet, participantId)
        else EitherT.rightT[FutureUnlessShutdown, ParticipantTopologyManagerError](false)
    } yield {
      if (appeared) {
        logger.debug("Packages appeared on all connected domains.")
      }
    }
  }

  override def revokeVettingForPackages(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: DarDescriptor,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Unit] = {
    val op = TopologyChangeOp.Remove
    val mapping = VettedPackages(participantId, packages)

    for {
      tx <- topologyManager
        .genTransaction(op, mapping, protocolVersion)
        .leftMap(ParticipantTopologyManagerError.IdentityManagerParentError(_))
        .leftMap { err =>
          logger.info(s"Unable to automatically revoke the vetting the dar $darDescriptor.")
          new DarUnvettingError(err, darDescriptor, mainPkg)
        }
        .leftWiden[CantonError]
        .mapK(FutureUnlessShutdown.outcomeK)

      _ = logger.debug(s"Revoking vetting for DAR $darDescriptor")

      _ <- topologyManager
        .authorize(tx, signingKey = None, protocolVersion, force = true)
        .leftMap(new DarUnvettingError(_, darDescriptor, mainPkg))
        .leftWiden[CantonError]
    } yield ()
  }
}
