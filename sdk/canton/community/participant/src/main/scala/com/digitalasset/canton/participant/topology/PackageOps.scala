// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.base.error.CantonRpcError
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.PackageInUse
import com.digitalasset.canton.participant.admin.PackageService.DarDescription
import com.digitalasset.canton.participant.admin.PackageVettingSynchronization
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ContinueAfterFailure, EitherTUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.Ref.PackageId

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

trait PackageOps extends NamedLogging {
  def hasVettedPackageEntry(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonRpcError, Boolean]

  def checkPackageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageInUse, Unit]

  def vetPackages(
      packages: Seq[PackageId],
      synchronizeVetting: PackageVettingSynchronization,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]

  def revokeVettingForPackages(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: DarDescription,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonRpcError, Unit]
}

class PackageOpsImpl(
    val participantId: ParticipantId,
    val headAuthorizedTopologySnapshot: TopologySnapshot,
    stateManager: SyncPersistentStateManager,
    topologyManager: AuthorizedTopologyManager,
    nodeId: UniqueIdentifier,
    initialProtocolVersion: ProtocolVersion,
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
)(implicit val ec: ExecutionContext)
    extends PackageOps
    with FlagCloseable {

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
    stateManager.getAll.toList
      // Sort to keep tests deterministic
      .sortBy { case (synchronizerId, _) => synchronizerId.toProtoPrimitive }
      .parTraverse_ { case (_, state) =>
        EitherT(
          state.activeContractStore
            .packageUsage(packageId, stateManager.contractStore.value)
            .map(opt =>
              opt.fold(Either.unit[PackageInUse])(contractId =>
                Left(
                  new PackageInUse(packageId, contractId, state.indexedSynchronizer.synchronizerId)
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
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, CantonRpcError, Boolean] = {
    // Use the aliasManager to query all synchronizers, even those that are currently disconnected
    val snapshotsForSynchronizers: List[TopologySnapshot] =
      stateManager.getAll.view.keys
        .map(stateManager.topologyFactoryFor(_, initialProtocolVersion))
        .flatMap(_.map(_.createHeadTopologySnapshot()))
        .toList

    val packageHasVettingEntry = (headAuthorizedTopologySnapshot :: snapshotsForSynchronizers)
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
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    vettingExecutionQueue.executeEUS(
      {
        val packagesToBeAdded = new AtomicReference[Seq[PackageId]](List.empty)
        for {
          newVettedPackagesCreated <- modifyVettedPackages { existingPackages =>
            // Keep deterministic order for testing and keep optimal O(n)
            val existingPackagesSet = existingPackages.map(_.packageId).toSet
            packagesToBeAdded.set(packages.filterNot(existingPackagesSet))
            existingPackages ++ VettedPackage.unbounded(packagesToBeAdded.get)
          }
          // only synchronize with the connected synchronizers if a new VettedPackages transaction was actually issued
          _ <- EitherTUtil.ifThenET(newVettedPackagesCreated) {
            synchronizeVetting.sync(packagesToBeAdded.get.toSet).mapK(FutureUnlessShutdown.outcomeK)
          }
        } yield ()

      },
      "vet packages",
    )

  override def revokeVettingForPackages(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: DarDescription,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, CantonRpcError, Unit] =
    vettingExecutionQueue.executeEUS(
      {
        val packagesToUnvet = packages.toSet

        modifyVettedPackages(_.filterNot(vp => packagesToUnvet(vp.packageId)))
          .leftWiden[CantonRpcError]
          .void
      },
      "revoke vetting",
    )

  /** Returns true if a new VettedPackages transaction was authorized. modifyVettedPackages should
    * not be called concurrently
    */
  private def modifyVettedPackages(
      action: Seq[VettedPackage] => Seq[VettedPackage]
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Boolean] =
    for {
      currentMapping <- EitherT.right(
        performUnlessClosingUSF(functionFullName)(
          topologyManager.store
            .findPositiveTransactions(
              asOf = CantonTimestamp.MaxValue,
              asOfInclusive = true,
              isProposal = false,
              types = Seq(VettedPackages.code),
              filterUid = Some(Seq(nodeId)),
              filterNamespace = None,
            )
            .map { result =>
              result
                .collectOfMapping[VettedPackages]
                .result
                .lastOption
            }
        )
      )
      currentPackages = currentMapping
        .map(_.mapping.packages)
        .getOrElse(Seq.empty)
      nextSerial = currentMapping.map(_.serial.increment)
      newVettedPackagesState = action(currentPackages)
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
      _ <- EitherTUtil.ifThenET(newVettedPackagesState != currentPackages) {
        performUnlessClosingEitherUSF(functionFullName)(
          topologyManager
            .proposeAndAuthorize(
              op = TopologyChangeOp.Replace,
              mapping = mapping,
              serial = nextSerial,
              signingKeys = Seq.empty,
              protocolVersion = initialProtocolVersion,
              expectFullAuthorization = true,
              forceChanges = ForceFlags(ForceFlag.AllowUnvetPackage),
              waitToBecomeEffective = None,
            )
            .leftMap(IdentityManagerParentError(_): ParticipantTopologyManagerError)
            .map(_ => ())
        )
      }
    } yield newVettedPackagesState != currentPackages
}
