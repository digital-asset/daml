// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.implicits.toFunctorOps
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageMissingDependencies
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.PackageInUse
import com.digitalasset.canton.participant.admin.PackageService.DarDescriptor
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.{
  IdentityManagerParentError,
  MainDarPackageReferencedExternally,
}
import com.digitalasset.canton.participant.topology.{
  ParticipantTopologyManager,
  ParticipantTopologyManagerError,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

trait PackageOps extends NamedLogging {

  /** @return true if the provided package-id is vetted or check-only in any of the participant's domain stores
    *         (i.e. it is known topology-wise). False otherwise
    */
  def isPackageKnown(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Boolean]

  def checkPackageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageInUse, Unit]

  def enableDarPackages(
      mainPkg: LfPackageId,
      dependencyPackageIds: Seq[PackageId],
      darDescription: String,
      synchronize: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]

  def disableDarPackages(
      mainPkg: LfPackageId,
      dependencyPackageIds: List[LfPackageId],
      darDescription: String,
      synchronize: Boolean,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]

  def fullyUnvet(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: DarDescriptor,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]
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

  override def isPackageKnown(
      packageId: PackageId
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, CantonError, Boolean] = {
    // Use the aliasManager to query all domains, even those that are currently disconnected
    val snapshotsForDomains: List[TopologySnapshot] =
      stateManager.getAll.view.keys
        .map(stateManager.topologyFactoryFor)
        .flatMap(_.map(_.createHeadTopologySnapshot()))
        .toList

    val packageKnown =
      (headAuthorizedTopologySnapshot :: snapshotsForDomains)
        .parTraverse { snapshot =>
          for {
            vetted <- snapshot
              .findUnvettedPackagesOrDependencies(participantId, Set(packageId))
              .map(_.isEmpty)
            checkOnly <- snapshot
              .findPackagesOrDependenciesNotDeclaredAsCheckOnly(participantId, Set(packageId))
              .map(_.isEmpty)
          } yield vetted || checkOnly
        }

    packageKnown.bimap(PackageMissingDependencies.Reject(packageId, _), _.contains(true))
  }
}

class PackageOpsImpl(
    val participantId: ParticipantId,
    val headAuthorizedTopologySnapshot: TopologySnapshot,
    stateManager: SyncDomainPersistentStateManager,
    topologyManager: ParticipantTopologyManager,
    protocolVersion: ProtocolVersion,
    futureSupervisor: FutureSupervisor,
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit override val ec: ExecutionContext)
    extends PackageOpsCommon(participantId, headAuthorizedTopologySnapshot, stateManager) {

  // Sequential queue used to ensure non-interleaving of topology update operations that are not transactional
  // but include multiple topology transactions (e.g. enable/disable DAR packages).
  private val topologyUpdatesSequentialQueue = new SimpleExecutionQueue(
    name = "package-ops-queue",
    futureSupervisor = futureSupervisor,
    timeouts = timeouts,
    loggerFactory = loggerFactory,
  )
  private val noOp: EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    EitherT.pure[FutureUnlessShutdown, ParticipantTopologyManagerError](())

  override def enableDarPackages(
      mainPkg: LfPackageId,
      dependencyPackageIds: Seq[PackageId],
      darDescription: String,
      synchronize: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
    logger.debug(s"Enabling packages of DAR $darDescription")
    val allPackageIds = mainPkg +: dependencyPackageIds
    for {
      _ <- topologyUpdatesSequentialQueue.executeEUS(
        for {
          _ <- addMappingIfMissing(
            mapping = VettedPackages(participantId, allPackageIds),
            checkMissing =
              topologyManager.mappingExists(VettedPackages(participantId, allPackageIds)).map(!_),
          )
          _ <- removeMappingIfExists(
            CheckOnlyPackages(participantId, allPackageIds),
            targetProtocolVersion = {
              // Check-only packages are introduced with PV 7
              Ordering[ProtocolVersion].max(protocolVersion, ProtocolVersion.v7)
            },
          )
        } yield (),
        "enable DAR",
      )
      _ <- onSynchronizeWaitForPackagesState(
        synchronize = synchronize,
        subjectDescription = "packages of DAR",
        stateDescription = "vetted",
        darDescription = darDescription,
        waitForState = topologyManager
          .waitForPackagesBeingVetted(allPackageIds.toSet, participantId),
      )
      // For simplicity, we do not synchronize on removal of the CheckOnlyPackages
      // since it is not necessary for the transition from disabled to enabled to occur,
      // which is observable by ledger clients by the synchronization of the VettedPackages
      // to connected domains.
      // Revoking of the CheckOnlyPackages mapping is done only for house-keeping
    } yield ()
  }

  override def disableDarPackages(
      mainPkg: LfPackageId,
      dependencyPackageIds: List[LfPackageId],
      darDescription: String,
      synchronize: Boolean,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
    logger.debug(s"Disabling packages of DAR $darDescription")

    val allPackageIds = mainPkg +: dependencyPackageIds

    for {
      _ <- topologyUpdatesSequentialQueue.executeEUS(
        for {
          _ <- checkDarDisableAllowedByMainPackageVetting(mainPkg, darDescription)
          _ <- addMappingIfMissing(
            mapping = CheckOnlyPackages(participantId, allPackageIds),
            checkMissing = topologyManager
              .mappingExists(CheckOnlyPackages(participantId, allPackageIds))
              .map(!_),
            targetProtocolVersion = {
              // Check-only packages are introduced with PV 7
              Ordering[ProtocolVersion].max(protocolVersion, ProtocolVersion.v7)
            },
          )
          _ <- removeMappingIfExists(VettedPackages(participantId, allPackageIds))
        } yield (),
        "disable DAR",
      )
      checkOnlySyncF = onSynchronizeWaitForPackagesState(
        synchronize = synchronize && CheckOnlyPackages.supportedOnProtocolVersion(protocolVersion),
        subjectDescription = "packages of DAR",
        stateDescription = "check-only",
        darDescription = darDescription,
        waitForState =
          topologyManager.waitForPackagesMarkedAsCheckOnly(allPackageIds.toSet, participantId),
      )
      vettedSyncF = onSynchronizeWaitForPackagesState(
        synchronize = synchronize,
        subjectDescription = "main package of DAR",
        stateDescription = "unvetted",
        darDescription = darDescription,
        waitForState = topologyManager.waitForPackageBeingUnvetted(mainPkg, participantId),
      )
      // Wait in parallel for synchronization
      _ <- checkOnlySyncF
      _ <- vettedSyncF
    } yield ()
  }

  private def checkDarDisableAllowedByMainPackageVetting(
      mainPkg: LfPackageId,
      darDescription: String,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, MainDarPackageReferencedExternally.Reject, Unit] =
    EitherT
      .right(
        topologyManager.isPackageContainedInMultipleVettedTransactions(
          pid = participantId,
          packageId = mainPkg,
        )
      )
      .subflatMap {
        case true =>
          Left(
            MainDarPackageReferencedExternally
              .Reject(
                operationName = "DAR disabling",
                mainPackageId = mainPkg,
                darDescription = darDescription,
              )
          )
        case false => Right(())
      }

  private def onSynchronizeWaitForPackagesState(
      synchronize: => Boolean,
      subjectDescription: String,
      stateDescription: String,
      darDescription: String,
      waitForState: => EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Boolean],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    EitherTUtil.ifThenET(synchronize)(waitForState.flatMap {
      case true =>
        logger.debug(
          s"${subjectDescription.capitalize} '$darDescription' appeared as $stateDescription on all connected domains"
        )
        noOp
      case false =>
        EitherT.leftT[FutureUnlessShutdown, Unit](
          ParticipantTopologyManagerError.PackageTopologyStateUpdateTimeout
            .Reject(
              s"Timed out while waiting for the $subjectDescription '$darDescription' to appear as $stateDescription on all connected domains"
            ): ParticipantTopologyManagerError
        )
    })

  private def addMappingIfMissing(
      mapping: TopologyPackagesStateUpdateMapping,
      checkMissing: => Future[Boolean],
      targetProtocolVersion: ProtocolVersion = protocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    for {
      missing <- EitherT.right(checkMissing).mapK(FutureUnlessShutdown.outcomeK)
      _ <-
        if (missing) {
          logger.debug(s"Authorizing add topology state for mapping $mapping")
          topologyManager
            .authorize(
              transaction = TopologyStateUpdate.createAdd(mapping, targetProtocolVersion),
              signingKey = None,
              protocolVersion = targetProtocolVersion,
            )
            .void
        } else {
          logger.debug(s"Skipping topology authorization as mapping $mapping already exists")
          noOp
        }
    } yield ()

  private def removeMappingIfExists(
      mapping: TopologyPackagesStateUpdateMapping,
      targetProtocolVersion: ProtocolVersion = protocolVersion,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    for {
      exists <- EitherT
        .right(topologyManager.mappingExists(mapping))
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <-
        if (exists) {
          logger.debug(s"Authorizing removal of topology state for mapping $mapping")
          for {
            removeVettingTx <- topologyManager
              .genTransaction(TopologyChangeOp.Remove, mapping, targetProtocolVersion)
              .leftMap(err => IdentityManagerParentError(err))
              .mapK(FutureUnlessShutdown.outcomeK)
            _ <- topologyManager.authorize(
              removeVettingTx,
              signingKey = None,
              targetProtocolVersion,
              force = true,
            )
          } yield ()
        } else {
          logger.debug(
            s"Skipping removal of mapping $mapping as it did not exist in the first place"
          )
          noOp
        }
    } yield ()

  def fullyUnvet(
      mainPkg: LfPackageId,
      dependencyPackages: List[LfPackageId],
      darDescriptor: DarDescriptor,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
    logger.debug(
      s"Removing all package topology transactions pertaining to DAR '${darDescriptor.toString}''"
    )
    val allPackages = mainPkg :: dependencyPackages
    topologyUpdatesSequentialQueue.executeEUS(
      for {
        _ <- removeMappingIfExists(CheckOnlyPackages(participantId, allPackages))
        _ <- removeMappingIfExists(VettedPackages(participantId, allPackages))
      } yield (),
      "unvetPackages",
    )
  }
}
