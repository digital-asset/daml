// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.ledger.api.{
  ListVettedPackagesOpts,
  PageToken,
  ParticipantVettedPackages,
  PriorTopologySerial,
  PriorTopologySerialExists,
  PriorTopologySerialNone,
  SinglePackageTargetVetting,
  UpdateVettedPackagesForceFlags,
}
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
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ContinueAfterFailure, SimpleExecutionQueue}
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
      expectedTopologySerial: Option[PriorTopologySerial],
      updateForceFlags: Option[UpdateVettedPackagesForceFlags] = None,
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    (Option[ParticipantVettedPackages], Option[ParticipantVettedPackages]),
  ]

  def getVettedPackages(
      opts: ListVettedPackagesOpts
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Seq[ParticipantVettedPackages]]
}

class PackageOpsImpl(
    val participantId: ParticipantId,
    stateManager: SyncPersistentStateManager,
    topologyManagerLookup: TopologyManagerLookup,
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
    modifyVettedPackages(psid, synchronizeVetting, ForceFlags.none, "vet packages") {
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
    }.void

  override def revokeVettingForPackages(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: DarDescription,
      psid: PhysicalSynchronizerId,
      forceFlags: ForceFlags,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, RpcError, Unit] = {
    val packagesToUnvet = packages.toSet
    modifyVettedPackages(
      psid = psid,
      synchronizeVetting = PackageVettingSynchronization.NoSync,
      forceFlags = forceFlags,
      operationName = "revoke vetting",
    )(_.filterNot(vp => packagesToUnvet(vp.packageId))).leftWiden[RpcError].void
  }

  override def updateVettedPackages(
      targetStates: Seq[SinglePackageTargetVetting[PackageId]],
      psid: PhysicalSynchronizerId,
      synchronizeVetting: PackageVettingSynchronization,
      dryRunSnapshot: Option[PackageMetadata],
      expectedTopologySerial: Option[PriorTopologySerial],
      updateForceFlags: Option[UpdateVettedPackagesForceFlags] = None,
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    (Option[ParticipantVettedPackages], Option[ParticipantVettedPackages]),
  ] = {
    val targetStatesMap: Map[PackageId, SinglePackageTargetVetting[PackageId]] =
      targetStates.map((x: SinglePackageTargetVetting[PackageId]) => x.ref -> x).toMap

    def toNextState(previousState: VettedPackage) =
      targetStatesMap.get(previousState.packageId) match {
        case None => Some(previousState)
        case Some(target) => target.toVettedPackage
      }

    val forceFlags = updateForceFlags.map(_.toForceFlags).getOrElse(ForceFlags.none)

    modifyVettedPackages(
      psid = psid,
      synchronizeVetting = synchronizeVetting,
      forceFlags = forceFlags,
      operationName = "update vetted packages",
      dryRunSnapshot = dryRunSnapshot,
      expectedTopologySerial = expectedTopologySerial,
    ) { currentVettedPackages =>
      val notInCurrentPackages = targetStatesMap -- currentVettedPackages.map(_.packageId)
      currentVettedPackages.flatMap(toNextState) ++ notInCurrentPackages.values.flatMap(
        _.toVettedPackage
      )
    }
  }

  override def getVettedPackages(
      opts: ListVettedPackagesOpts
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Seq[
    ParticipantVettedPackages
  ]] = {
    val synchronizers =
      opts.synchronizers.map(_.forgetNE).getOrElse(stateManager.getAllLogical.keys).toSet
    val pageSynchronizers =
      opts.pageToken.sortAndFilterSynchronizers(synchronizers, opts.participants)

    pageSynchronizers
      .foldM(opts.pageSize.value -> Seq.empty[ParticipantVettedPackages]) {
        case (
              state @ (remainingPageSize, resultsSoFar),
              (synchronizerId, participantStartExclusive, participantsFilter),
            ) =>
          if (remainingPageSize <= 0)
            EitherT.pure[FutureUnlessShutdown, ParticipantTopologyManagerError](state)
          else
            for {
              topologyManager <- topologyManagerLookup.activeBySynchronizerId(synchronizerId)
              newResultsWithinPage <- getVettedPackagesForSynchronizer(
                topologyManager = topologyManager,
                participantsFilter = participantsFilter,
                pageLimit = remainingPageSize,
                participantStartExclusive = participantStartExclusive,
              )
            } yield {
              val newRemainingPageSize = remainingPageSize - newResultsWithinPage.length
              val newResultsSoFar = resultsSoFar ++ newResultsWithinPage
              (newRemainingPageSize, newResultsSoFar)
            }
      }
      .map(_._2)
  }

  private def getVettedPackagesForSynchronizer(
      topologyManager: SynchronizerTopologyManager,
      participantsFilter: Option[NonEmpty[Set[ParticipantId]]],
      pageLimit: Int,
      participantStartExclusive: Option[ParticipantId] = None,
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    Seq[ParticipantVettedPackages],
  ] = for {
    vettedPackages <- EitherT.right(
      synchronizeWithClosing(functionFullName)(
        topologyManager.store
          .findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = true,
            isProposal = false,
            types = Seq(VettedPackages.code),
            filterUid = participantsFilter.map(_.toSeq.map(_.uid)),
            filterNamespace = None,
            pagination = Some((participantStartExclusive.map(_.uid), pageLimit)),
          )
      )
    )

    transactions = vettedPackages.collectOfMapping[VettedPackages].result

    participantVettedPackages = transactions
      .map { currentMapping =>
        ParticipantVettedPackages(
          currentMapping.mapping.packages,
          currentMapping.mapping.participantId,
          topologyManager.psid.logical,
          currentMapping.serial,
        )
      }
      .sorted(PageToken.orderingVettedPackages)
  } yield participantVettedPackages

  private def checkCurrentSerial(
      currentSerial: Option[PositiveInt],
      expectedSerial: Option[PriorTopologySerial],
  )(implicit tc: TraceContext): Either[ParticipantTopologyManagerError, Unit] =
    expectedSerial match {
      case None =>
        Right(()) // no check required
      case Some(PriorTopologySerialNone) =>
        // check there is no prior serial
        Either.cond(
          currentSerial.isEmpty,
          (),
          IdentityManagerParentError(
            TopologyManagerError.SerialMismatch.Failure(
              actual = currentSerial,
              expected = None,
            )
          ),
        )
      case Some(PriorTopologySerialExists(expectedSerial)) =>
        // check expected serial matches current serial
        Either.cond(
          currentSerial.contains(expectedSerial),
          (),
          IdentityManagerParentError(
            TopologyManagerError.SerialMismatch.Failure(
              actual = currentSerial,
              expected = Some(expectedSerial),
            )
          ),
        )
    }

  /** Modifies the existing vetted packages for the specified synchronizer and returns the
    * before-and-after participant vetting state.
    */
  private def modifyVettedPackages(
      psid: PhysicalSynchronizerId,
      synchronizeVetting: PackageVettingSynchronization,
      forceFlags: ForceFlags,
      operationName: String,
      // If defined, do not persist changes, just validate them
      dryRunSnapshot: Option[PackageMetadata] = None,
      expectedTopologySerial: Option[PriorTopologySerial] = None,
  )(
      action: Seq[VettedPackage] => Seq[VettedPackage]
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    (Option[ParticipantVettedPackages], Option[ParticipantVettedPackages]),
  ] =
    vettingExecutionQueue.executeEUS(
      description = operationName,
      execution = for {
        topologyManager <- topologyManagerLookup.byPhysicalSynchronizerId(psid)

        currentState <-
          getVettedPackagesForSynchronizer(topologyManager, Some(NonEmpty(Set, participantId)), 1)
            .map(_.headOption)

        currentVettedPackages = currentState.map(_.packages)
        currentVettedPackagesSet = currentVettedPackages.map(_.toSet)
        currentSerial = currentState.map(_.serial)

        _ <- EitherT.fromEither[FutureUnlessShutdown](
          checkCurrentSerial(currentSerial, expectedSerial = expectedTopologySerial)
        )
        newVettedPackages = action(currentVettedPackages.getOrElse(Seq.empty))
        newVettedPackagesSet = newVettedPackages.toSet

        dryRun = dryRunSnapshot.isDefined
        nextSerial = currentSerial.map(_.increment).getOrElse(PositiveInt.one)
        nextParticipantState = ParticipantVettedPackages(
          packages = newVettedPackages,
          participantId = participantId,
          synchronizerId = psid.logical,
          serial = nextSerial,
        )
        newState <-
          if (!currentVettedPackagesSet.contains(newVettedPackagesSet) && !dryRun)
            setVettedPackages(
              forceFlags = forceFlags,
              topologyManager = topologyManager,
              nextSerial = nextSerial,
              newVettedPackagesState = newVettedPackages,
              synchronizeVetting = synchronizeVetting,
            ).map(nextSerial => Some(nextParticipantState.copy(serial = nextSerial)))
          else if (dryRun)
            topologyManager
              .validatePackageVetting(
                currentlyVettedPackages =
                  currentVettedPackagesSet.getOrElse(Set.empty).map(_.packageId),
                nextPackageIds = newVettedPackagesSet.map(_.packageId),
                dryRunSnapshot = dryRunSnapshot,
                forceFlags = forceFlags,
              )
              .leftMap[ParticipantTopologyManagerError](IdentityManagerParentError(_))
              .map { _ =>
                if (currentVettedPackagesSet.contains(newVettedPackagesSet)) currentState
                else Some(nextParticipantState)
              }
          else
            EitherT.pure[FutureUnlessShutdown, ParticipantTopologyManagerError](currentState)
      } yield currentState -> newState,
    )

  private def setVettedPackages(
      forceFlags: ForceFlags,
      topologyManager: SynchronizerTopologyManager,
      nextSerial: PositiveInt,
      newVettedPackagesState: Seq[VettedPackage],
      synchronizeVetting: PackageVettingSynchronization,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, PositiveInt] =
    for {
      mapping <-
        VettedPackages
          .create(participantId, newVettedPackagesState)
          .toEitherT[FutureUnlessShutdown]
          .leftMap(err =>
            ParticipantTopologyManagerError.IdentityManagerParentError(
              TopologyManagerError.InvalidTopologyMapping.Reject(err)
            )
          )
      signedTx <- synchronizeWithClosing(functionFullName)(
        topologyManager
          .proposeAndAuthorize(
            op = TopologyChangeOp.Replace,
            mapping = mapping,
            serial = Some(nextSerial),
            signingKeys = Seq.empty,
            protocolVersion = initialProtocolVersion,
            expectFullAuthorization = true,
            forceChanges = forceFlags,
            waitToBecomeEffective = None,
          )
          .leftMap[ParticipantTopologyManagerError](IdentityManagerParentError(_))
      )
      _ <- synchronizeVetting
        .sync(newVettedPackagesState.toSet, topologyManager.psid)
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield signedTx.transaction.serial
}

object PackageOpsImpl {
  implicit class TargetVettingToVettedPackage(target: SinglePackageTargetVetting[PackageId]) {
    def toVettedPackage: Option[VettedPackage] =
      target.bounds.map { case (lower, upper) => VettedPackage(target.ref, lower, upper) }
  }
}
