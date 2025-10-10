// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.SinglePackageTargetVetting
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.PackageInUse
import com.digitalasset.canton.participant.admin.PackageService.DarDescription
import com.digitalasset.canton.participant.admin.PackageVettingSynchronization
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.store.StoredTopologyTransaction
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ContinueAfterFailure, EitherTUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.ExecutionContext

trait PackageOps extends NamedLogging {
  def hasVettedPackageEntry(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Boolean]

  def checkPackageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageInUse, Unit]

  def vetPackages(
      packages: Seq[PackageId],
      synchronizeVetting: PackageVettingSynchronization,
      psid: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]

  def revokeVettingForPackages(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: DarDescription,
      psid: PhysicalSynchronizerId,
      forceFlags: ForceFlags,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit]

  def updateVettedPackages(
      targetStates: Seq[SinglePackageTargetVetting[PackageId]],
      psid: PhysicalSynchronizerId,
      synchronizeVetting: PackageVettingSynchronization,
      dryRunSnapshot: Option[PackageMetadata],
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    (Seq[VettedPackage], Seq[VettedPackage]),
  ]

  def getVettedPackages(synchronizerFilter: Option[Set[SynchronizerId]])(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    Seq[(Seq[VettedPackage], SynchronizerId, PositiveInt)],
  ]

  def getVettedPackagesForSynchronizer(
      topologyManager: SynchronizerTopologyManager
  )(implicit tc: TraceContext): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    Option[(Seq[VettedPackage], PositiveInt)],
  ]
}

class PackageOpsImpl(
    val participantId: ParticipantId,
    stateManager: SyncPersistentStateManager,
    topologyManagerLookup: TopologyManagerLookup,
    nodeId: UniqueIdentifier,
    initialProtocolVersion: ProtocolVersion,
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
)(implicit val ec: ExecutionContext)
    extends PackageOps
    with FlagCloseable {
  import PackageOpsImpl.*

  private val vettingExecutionQueue = new SimpleExecutionQueue(
    "sequential-vetting-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    logTaskTiming = false,
    failureMode = ContinueAfterFailure,
  )

  override def checkPackageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageInUse, Unit] =
    // Restricting to latest physical state because only (active) contract stores are used
    stateManager.getAllLatest.toList
      // Sort to keep tests deterministic
      .sortBy { case (synchronizerId, _) => synchronizerId.toProtoPrimitive }
      .parTraverse_ { case (_, state) =>
        EitherT(
          state.activeContractStore
            .packageUsage(packageId, stateManager.contractStore.value)
            .map(opt =>
              opt.fold(Either.unit[PackageInUse])(contractId =>
                Left(
                  new PackageInUse(
                    packageId,
                    contractId,
                    state.synchronizerIdx.synchronizerId,
                  )
                )
              )
            )
        )
      }

  /** @return
    *   true if the authorized snapshot, or any synchronizer snapshot has a package vetting entry
    *   for the package regardless of the validity period of the package.
    */
  override def hasVettedPackageEntry(
      packageId: PackageId
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, RpcError, Boolean] = {
    // Use the aliasManager to query all synchronizers, even those that are currently disconnected
    val snapshotsForSynchronizers: List[TopologySnapshot] =
      stateManager.getAll.view.values
        .map(persistentState => stateManager.topologyFactoryFor(persistentState.psid))
        .flatMap(_.map(_.createHeadTopologySnapshot()))
        .toList

    val packageHasVettingEntry = snapshotsForSynchronizers
      .parTraverse { snapshot =>
        snapshot
          .determinePackagesWithNoVettingEntry(participantId, Set(packageId))
          .map(_.isEmpty)
      }

    EitherT.right(packageHasVettingEntry.map(_.contains(true)))
  }

  override def vetPackages(
      packages: Seq[PackageId],
      synchronizeVetting: PackageVettingSynchronization,
      psid: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    vettingExecutionQueue.executeEUS(
      for {
        newVettedPackagesCreated <- modifyVettedPackages(psid, ForceFlags.none) {
          existingPackages =>
            val existingAndUpdatedPackages = existingPackages.map { existingVettedPackage =>
              // if a package to vet has been previously vetted, make sure it has no time bounds
              if (packages.contains(existingVettedPackage.packageId))
                existingVettedPackage.asUnbounded
              else existingVettedPackage
            }
            // now determine the actually new packages that need to be vetted
            val actuallyNewPackages =
              VettedPackage.unbounded(packages).toSet -- existingAndUpdatedPackages
            existingAndUpdatedPackages ++ actuallyNewPackages
        }
        // only synchronize with the connected synchronizers if a new VettedPackages transaction was actually issued
        _ <- newVettedPackagesCreated.traverse_ { vettedPackages =>
          synchronizeVetting
            .sync(vettedPackages, psid)
            .mapK(FutureUnlessShutdown.outcomeK)
        }
      } yield (),
      "vet packages",
    )

  override def revokeVettingForPackages(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: DarDescription,
      psid: PhysicalSynchronizerId,
      forceFlags: ForceFlags,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    vettingExecutionQueue.executeEUS(
      {
        val packagesToUnvet = packages.toSet

        modifyVettedPackages(psid, forceFlags)(_.filterNot(vp => packagesToUnvet(vp.packageId)))
          .leftWiden[RpcError]
          .void
      },
      "revoke vetting",
    )

  override def updateVettedPackages(
      targetStates: Seq[SinglePackageTargetVetting[PackageId]],
      psid: PhysicalSynchronizerId,
      synchronizeVetting: PackageVettingSynchronization,
      dryRunSnapshot: Option[PackageMetadata],
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    (Seq[VettedPackage], Seq[VettedPackage]),
  ] =
    vettingExecutionQueue.executeEUS(
      {
        val targetStatesMap: Map[PackageId, SinglePackageTargetVetting[PackageId]] =
          targetStates.map((x: SinglePackageTargetVetting[PackageId]) => x.ref -> x).toMap

        def toChange(previousState: VettedPackage): VettedPackageChange =
          targetStatesMap.get(previousState.packageId) match {
            case None => VettedPackageChange.Unchanged(previousState)
            case Some(target) =>
              VettedPackageChange.Changed(Some(previousState), target.toVettedPackage)
          }

        for {
          topologyManager <- topologyManagerLookup.byPhysicalSynchronizerId(psid)
          currentPackagesAndSerial <- getVettedPackagesForSynchronizer(topologyManager)
          currentPackages = currentPackagesAndSerial.map(_._1).getOrElse(Seq())
          currentSerial = currentPackagesAndSerial.map(_._2)

          notInCurrentPackages = targetStatesMap -- currentPackages.map(_.packageId)
          updateInstructions =
            currentPackages.map(toChange) ++ notInCurrentPackages.values.map(
              _.toFreshVettedPackageChange
            )
          newAllPackages = updateInstructions.flatMap(_.newState)
          newVettedPackagesCreated <-
            if (dryRunSnapshot.isDefined) {
              topologyManager
                .validatePackageVetting(
                  currentlyVettedPackages = currentPackages.map(_.packageId).toSet,
                  nextPackageIds = newAllPackages.map(_.packageId).toSet,
                  dryRunSnapshot = dryRunSnapshot,
                  forceFlags = ForceFlags.none,
                )
                .leftMap[ParticipantTopologyManagerError](IdentityManagerParentError(_))
                .map(_ => Option.empty[Set[VettedPackage]])
            } else {
              // Fails if a new topology change is submitted between getVettedPackages
              // above and this call to setVettedPackages, since currentSerial will no
              // longer be valid.
              setVettedPackages(
                topologyManager,
                currentPackages,
                newAllPackages,
                currentSerial,
                ForceFlags.none,
              )
            }
          // only synchronize with the connected synchronizers if a new VettedPackages transaction was actually issued
          _ <- newVettedPackagesCreated.traverse_ { newVettedPackages =>
            synchronizeVetting
              .sync(newVettedPackages, psid)
              .mapK(FutureUnlessShutdown.outcomeK)
          }
        } yield (
          currentPackages,
          newAllPackages,
        )
      },
      "update vetted packages",
    )

  override def getVettedPackages(synchronizerFilter: Option[Set[SynchronizerId]])(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    Seq[(Seq[VettedPackage], SynchronizerId, PositiveInt)],
  ] = {
    val synchronizers = synchronizerFilter.getOrElse(stateManager.getAllLogical.keySet)
    synchronizers.toSeq
      .parFlatTraverse { synchronizerId =>
        for {
          topologyManager <- topologyManagerLookup.activeBySynchronizerId(synchronizerId)
          resultO <- getVettedPackagesForSynchronizer(topologyManager)
        } yield resultO.map { case (packages, serial) => (packages, synchronizerId, serial) }.toList
      }
  }

  override def getVettedPackagesForSynchronizer(topologyManager: SynchronizerTopologyManager)(
      implicit tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    Option[(Seq[VettedPackage], PositiveInt)],
  ] =
    EitherT.right(
      synchronizeWithClosing(functionFullName)(
        topologyManager.store
          .findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = true,
            isProposal = false,
            types = Seq(VettedPackages.code),
            filterUid = Some(NonEmpty(Seq, nodeId)),
            filterNamespace = None,
          )
          .map { result =>
            result
              .collectOfMapping[VettedPackages]
              .result
              .lastOption
              .map {
                (currentMapping: StoredTopologyTransaction[
                  TopologyChangeOp.Replace,
                  VettedPackages,
                ]) =>
                  (currentMapping.mapping.packages, currentMapping.serial)
              }
          }
      )
    )

  /** Returns true if a new VettedPackages transaction was authorized. modifyVettedPackages should
    * not be called concurrently
    */
  private def modifyVettedPackages(psid: PhysicalSynchronizerId, forceFlags: ForceFlags)(
      action: Seq[VettedPackage] => Seq[VettedPackage]
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Option[Set[VettedPackage]]] =
    for {
      topologyManager <- topologyManagerLookup.byPhysicalSynchronizerId(psid)
      currentPackagesAndSerial <- getVettedPackagesForSynchronizer(topologyManager)
      currentPackages = currentPackagesAndSerial.map(_._1).getOrElse(Seq())
      currentSerial = currentPackagesAndSerial.map(_._2)

      newVettedPackagesState = action(currentPackages)
      result <- setVettedPackages(
        topologyManager,
        currentPackages,
        newVettedPackagesState,
        currentSerial,
        forceFlags,
      )
    } yield result

  private def setVettedPackages(
      topologyManager: SynchronizerTopologyManager,
      currentPackages: Seq[VettedPackage],
      newVettedPackagesState: Seq[VettedPackage],
      currentSerial: Option[PositiveInt],
      forceFlags: ForceFlags,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Option[Set[VettedPackage]]] =
    for {
      mapping <- EitherT
        .fromEither[FutureUnlessShutdown](
          VettedPackages.create(
            participantId = participantId,
            newVettedPackagesState,
          )
        )
        .leftMap(err =>
          ParticipantTopologyManagerError.IdentityManagerParentError(
            TopologyManagerError.InvalidTopologyMapping.Reject(err)
          )
        )
      newSerial = currentSerial.map(_.increment)
      _ <- EitherTUtil.ifThenET(newVettedPackagesState != currentPackages) {
        synchronizeWithClosing(functionFullName)(
          topologyManager
            .proposeAndAuthorize(
              op = TopologyChangeOp.Replace,
              mapping = mapping,
              serial = newSerial,
              signingKeys = Seq.empty,
              protocolVersion = initialProtocolVersion,
              expectFullAuthorization = true,
              forceChanges = forceFlags,
              waitToBecomeEffective = None,
            )
            .leftMap[ParticipantTopologyManagerError](IdentityManagerParentError(_))
        )
      }
    } yield Option.when(newVettedPackagesState != currentPackages)(newVettedPackagesState.toSet)
}

object PackageOpsImpl {
  sealed trait VettedPackageChange {
    def newState: Option[VettedPackage]
  }

  object VettedPackageChange {
    final case class Unchanged(state: VettedPackage) extends VettedPackageChange {
      override def newState = Some(state)
    }

    final case class Changed(
        previousState: Option[VettedPackage],
        newState: Option[VettedPackage],
    ) extends VettedPackageChange
  }

  implicit class TargetVettingToVettedPackage(target: SinglePackageTargetVetting[PackageId]) {
    def toVettedPackage: Option[VettedPackage] =
      target.bounds.map { case (lower, upper) => VettedPackage(target.ref, lower, upper) }

    def toFreshVettedPackageChange: VettedPackageChange.Changed =
      VettedPackageChange.Changed(None, toVettedPackage)
  }
}
