// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.implicits.toBifunctorOps
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageMissingDependencies
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.PackageInUse
import com.digitalasset.canton.participant.admin.PackageService.DarDescriptor
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{AuthorizedTopologyManager, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.Ref.PackageId

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

trait PackageOps extends NamedLogging {
  def isPackageVetted(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Boolean]

  def checkPackageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageInUse, Unit]

  def vetPackages(
      packages: Seq[PackageId],
      synchronizeVetting: PackageVettingSynchronization,
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

class PackageOpsImpl(
    val participantId: ParticipantId,
    val headAuthorizedTopologySnapshot: TopologySnapshot,
    stateManager: SyncDomainPersistentStateManager,
    topologyManager: AuthorizedTopologyManager,
    nodeId: UniqueIdentifier,
    initialProtocolVersion: ProtocolVersion,
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
)(implicit val ec: ExecutionContext)
    extends PackageOps
    with FlagCloseable {

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

  /** @return true if the authorized snapshot, or any domain snapshot has the package vetted */
  override def isPackageVetted(
      packageId: PackageId
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, CantonError, Boolean] = {
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
          .map(_.isEmpty)
      }

    packageIsVettedOn.bimap(PackageMissingDependencies.Reject(packageId, _), _.contains(true))
  }

  override def vetPackages(
      packages: Seq[PackageId],
      synchronizeVetting: PackageVettingSynchronization,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
    val packagesToBeAdded = new AtomicReference[Seq[PackageId]](List.empty)
    for {
      newVettedPackagesCreated <- modifyVettedPackages { existingPackages =>
        // Keep deterministic order for testing and keep optimal O(n)
        val existingPackagesSet = existingPackages.toSet
        packagesToBeAdded.set(packages.filterNot(existingPackagesSet))
        existingPackages ++ packagesToBeAdded.get
      }
      // only synchronize with the connected domains if a new VettedPackages transaction was actually issued
      _ <- EitherTUtil.ifThenET(newVettedPackagesCreated) {
        synchronizeVetting.sync(packagesToBeAdded.get.toSet)
      }
    } yield ()

  }

  override def revokeVettingForPackages(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: DarDescriptor,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, CantonError, Unit] = {
    val packagesToUnvet = packages.toSet

    modifyVettedPackages(_.filterNot(packagesToUnvet)).leftWiden[CantonError].void
  }

  /** Returns true if a new VettedPackages transaction was authorized. */
  def modifyVettedPackages(
      action: Seq[LfPackageId] => Seq[LfPackageId]
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Boolean] = {
    // TODO(#14069) this vetting extension might fail on concurrent requests

    for {
      currentMapping <- EitherT.right(
        performUnlessClosingF(functionFullName)(
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
        .map(_.mapping.packageIds)
        .getOrElse(Seq.empty)
      nextSerial = currentMapping.map(_.serial.increment)
      newVettedPackagesState = action(currentPackages)
      _ <- EitherTUtil.ifThenET(newVettedPackagesState != currentPackages) {
        performUnlessClosingEitherUSF(functionFullName)(
          topologyManager
            .proposeAndAuthorize(
              op = TopologyChangeOp.Replace,
              mapping = VettedPackages(
                participantId = participantId,
                domainId = None,
                newVettedPackagesState,
              ),
              serial = nextSerial,
              // TODO(#12390) auto-determine signing keys
              signingKeys = Seq(participantId.fingerprint),
              protocolVersion = initialProtocolVersion,
              expectFullAuthorization = true,
            )
            .leftMap(IdentityManagerParentError(_): ParticipantTopologyManagerError)
            .map(_ => ())
        )
      }
    } yield newVettedPackagesState != currentPackages
  }
}
