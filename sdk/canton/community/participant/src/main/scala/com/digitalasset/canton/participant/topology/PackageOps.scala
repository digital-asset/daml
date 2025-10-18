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
import com.digitalasset.canton.ledger.api.{
  InitialPageToken,
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
    vettingExecutionQueue.executeEUS(
      for {
        _ <- modifyVettedPackages(psid, synchronizeVetting, ForceFlags.none) { existingPackages =>
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

        modifyVettedPackages(
          psid,
          PackageVettingSynchronization.NoSync,
          forceFlags,
        )(_.filterNot(vp => packagesToUnvet(vp.packageId))).leftWiden[RpcError].void
      },
      "revoke vetting",
    )

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

        val forceFlags = updateForceFlags.map(_.toForceFlags).getOrElse(ForceFlags.none)

        for {
          topologyManager <- topologyManagerLookup.byPhysicalSynchronizerId(psid)
          currentPackagesAndSerial <-
            getVettedPackageForSynchronizerAndParticipant(topologyManager, participantId)
          currentPackages = currentPackagesAndSerial.map(_.packages).getOrElse(Seq.empty)
          currentSerial = currentPackagesAndSerial.map(_.serial)
          _ <- EitherT.fromEither[FutureUnlessShutdown](
            checkCurrentSerial(currentSerial, expectedSerial = expectedTopologySerial)
          )
          notInCurrentPackages = targetStatesMap -- currentPackages.map(_.packageId)
          updateInstructions =
            currentPackages.map(toChange) ++ notInCurrentPackages.values.map(
              _.toFreshVettedPackageChange
            )
          newAllPackages = updateInstructions.flatMap(_.newState)
          newPackagesAndSerial <-
            if (dryRunSnapshot.isDefined) {
              topologyManager
                .validatePackageVetting(
                  currentlyVettedPackages = currentPackages.map(_.packageId).toSet,
                  nextPackageIds = newAllPackages.map(_.packageId).toSet,
                  dryRunSnapshot = dryRunSnapshot,
                  forceFlags = forceFlags,
                )
                .leftMap[ParticipantTopologyManagerError](IdentityManagerParentError(_))
                .map { _ =>
                  if (newAllPackages == currentPackages) currentPackagesAndSerial
                  else {
                    val nextSerial = currentSerial.map(_.increment).getOrElse(PositiveInt.one)
                    Some(
                      ParticipantVettedPackages(
                        newAllPackages,
                        participantId,
                        psid.logical,
                        nextSerial,
                      )
                    )
                  }
                }
            } else {
              // Fails if a new topology change is submitted between getVettedPackages
              // above and this call to setVettedPackages, since currentSerial will no
              // longer be valid.
              setVettedPackages(
                topologyManager,
                currentPackagesAndSerial,
                newAllPackages,
                psid,
                synchronizeVetting,
                forceFlags = forceFlags,
              )
                .map(_.orElse(currentPackagesAndSerial))
            }
        } yield (currentPackagesAndSerial, newPackagesAndSerial)
      },
      "update vetted packages",
    )

  override def getVettedPackages(
      opts: ListVettedPackagesOpts
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    Seq[ParticipantVettedPackages],
  ] = {
    val sortedSynchronizers: Seq[SynchronizerId] =
      opts.filterSynchronizersInToken(default = stateManager.getAllLogical.keySet)
    val initialState: (Int, Seq[ParticipantVettedPackages]) = (opts.pageSize.value, Seq())

    sortedSynchronizers
      .foldM(initialState) { (state, synchronizerId) =>
        val (remainingPageSize, resultsSoFar) = state
        if (remainingPageSize <= 0)
          EitherT.pure[FutureUnlessShutdown, ParticipantTopologyManagerError](state)
        else
          opts.userSpecifiedParticipantsInToken(synchronizerId) match {
            case None =>
              EitherT.pure[FutureUnlessShutdown, ParticipantTopologyManagerError](state)
            case Some(userSpecifiedParticipants) =>
              for {
                topologyManager <- topologyManagerLookup.activeBySynchronizerId(synchronizerId)
                vettedPackages <- getVettedPackagesForSynchronizer(
                  topologyManager,
                  userSpecifiedParticipants,
                  pageToken = opts.pageToken,
                )
              } yield {
                val newResultsWithinPage = vettedPackages.take(remainingPageSize)
                val newRemainingPageSize = remainingPageSize - newResultsWithinPage.length
                (newRemainingPageSize, resultsSoFar ++ newResultsWithinPage)
              }
          }
      }
      .map(_._2)
  }

  private def getVettedPackageForSynchronizerAndParticipant(
      topologyManager: SynchronizerTopologyManager,
      participantId: ParticipantId,
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    Option[ParticipantVettedPackages],
  ] =
    getVettedPackagesForSynchronizer(topologyManager, Some(NonEmpty(Set, participantId)))
      .map(_.headOption)

  private def getVettedPackagesForSynchronizer(
      topologyManager: SynchronizerTopologyManager,
      participantIds: Option[NonEmpty[Set[ParticipantId]]],
      pageToken: PageToken = InitialPageToken,
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    Seq[ParticipantVettedPackages],
  ] =
    EitherT.right(
      synchronizeWithClosing(functionFullName)(
        topologyManager.store
          .findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = true,
            isProposal = false,
            types = Seq(VettedPackages.code),
            filterUid = participantIds.map(_.toSeq.map(_.uid)),
            filterNamespace = None,
          )
          .map { result =>
            val transactions = result
              .collectOfMapping[VettedPackages]
              .collectLatestByUniqueKey
              .result

            val vettedPackages =
              transactions
                .map { currentMapping =>
                  ParticipantVettedPackages(
                    currentMapping.mapping.packages,
                    currentMapping.mapping.participantId,
                    topologyManager.psid.logical,
                    currentMapping.serial,
                  )
                }

            pageToken.sortAndFilterVettedPackages(vettedPackages)
          }
      )
    )

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

  /** Returns true if a new VettedPackages transaction was authorized. modifyVettedPackages should
    * not be called concurrently
    */
  private def modifyVettedPackages(
      psid: PhysicalSynchronizerId,
      synchronizeVetting: PackageVettingSynchronization,
      forceFlags: ForceFlags,
  )(
      action: Seq[VettedPackage] => Seq[VettedPackage]
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Option[
    ParticipantVettedPackages
  ]] =
    for {
      topologyManager <- topologyManagerLookup.byPhysicalSynchronizerId(psid)
      currentPackagesAndSerial <-
        getVettedPackageForSynchronizerAndParticipant(topologyManager, participantId)
      currentPackages = currentPackagesAndSerial.map(_.packages).getOrElse(Seq.empty)

      newVettedPackagesState = action(currentPackages)
      result <- setVettedPackages(
        topologyManager,
        currentPackagesAndSerial,
        newVettedPackagesState,
        psid,
        synchronizeVetting,
        forceFlags,
      )
    } yield result

  private def setVettedPackages(
      topologyManager: SynchronizerTopologyManager,
      currentPackagesAndSerial: Option[ParticipantVettedPackages],
      newVettedPackagesState: Seq[VettedPackage],
      psid: PhysicalSynchronizerId,
      synchronizeVetting: PackageVettingSynchronization,
      forceFlags: ForceFlags,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Option[
    ParticipantVettedPackages
  ]] =
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
      newSerialOpt <-
        if (currentPackagesAndSerial.forall(_.packages != newVettedPackagesState)) {
          synchronizeWithClosing(functionFullName)(
            topologyManager
              .proposeAndAuthorize(
                op = TopologyChangeOp.Replace,
                mapping = mapping,
                serial = currentPackagesAndSerial.map(_.serial.increment),
                signingKeys = Seq.empty,
                protocolVersion = initialProtocolVersion,
                expectFullAuthorization = true,
                forceChanges = forceFlags,
                waitToBecomeEffective = None,
              )
              .leftMap[ParticipantTopologyManagerError](IdentityManagerParentError(_))
              .map[Option[PositiveInt]](signedTx => Some(signedTx.transaction.serial))
          )
        } else
          EitherT.pure[FutureUnlessShutdown, ParticipantTopologyManagerError](
            Option.empty[PositiveInt]
          )
      // only synchronize with the connected synchronizers if a new transaction was actually issued
      _ <- newSerialOpt.traverse_ { _ =>
        synchronizeVetting
          .sync(newVettedPackagesState.toSet, psid)
          .mapK(FutureUnlessShutdown.outcomeK)
      }
    } yield newSerialOpt.map(serial =>
      ParticipantVettedPackages(newVettedPackagesState, participantId, psid.logical, serial)
    )
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
