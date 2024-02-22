// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.implicits.toBifunctorOps
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
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
import com.digitalasset.canton.topology.{
  AuthorizedTopologyManagerX,
  ParticipantId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
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

// TODO(#15161) collapse with PackageOpsCommon
class PackageOpsX(
    val participantId: ParticipantId,
    val headAuthorizedTopologySnapshot: TopologySnapshot,
    manager: SyncDomainPersistentStateManager,
    topologyManager: AuthorizedTopologyManagerX,
    nodeId: UniqueIdentifier,
    initialProtocolVersion: ProtocolVersion,
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
)(implicit override val ec: ExecutionContext)
    extends PackageOpsCommon(participantId, headAuthorizedTopologySnapshot, manager)
    with FlagCloseable {

  override def vetPackages(packages: Seq[PackageId], synchronize: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
    modifyVettedPackages { existingPackages =>
      // Keep deterministic order for testing and keep optimal O(n)
      val existingPackagesSet = existingPackages.toSet
      val packagesToBeAdded = packages.filterNot(existingPackagesSet)
      existingPackages ++ packagesToBeAdded
    }
  }

  override def revokeVettingForPackages(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: DarDescriptor,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, CantonError, Unit] = {
    val packagesToUnvet = packages.toSet

    modifyVettedPackages(_.filterNot(packagesToUnvet)).leftWiden
  }

  def modifyVettedPackages(
      action: Seq[LfPackageId] => Seq[LfPackageId]
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
    // TODO(#14069) this vetting extension might fail on concurrent requests

    for {
      currentMapping <- EitherT.right(
        performUnlessClosingF(functionFullName)(
          topologyManager.store
            .findPositiveTransactions(
              asOf = CantonTimestamp.MaxValue,
              asOfInclusive = true,
              isProposal = false,
              types = Seq(VettedPackagesX.code),
              filterUid = Some(Seq(nodeId)),
              filterNamespace = None,
            )
            .map { result =>
              result
                .collectOfMapping[VettedPackagesX]
                .result
                .lastOption
            }
        )
      )
      currentPackages = currentMapping
        .map(_.transaction.transaction.mapping.packageIds)
        .getOrElse(Seq.empty)
      nextSerial = currentMapping.map(_.transaction.transaction.serial.increment)
      newVettedPackagesState = action(currentPackages)
      _ <- EitherTUtil.ifThenET(newVettedPackagesState != currentPackages) {
        performUnlessClosingEitherUSF(functionFullName)(
          topologyManager
            .proposeAndAuthorize(
              op = TopologyChangeOpX.Replace,
              mapping = VettedPackagesX(
                participantId = participantId,
                domainId = None,
                newVettedPackagesState,
              ),
              serial = nextSerial,
              // TODO(#12390) auto-determine signing keys
              signingKeys = Seq(participantId.uid.namespace.fingerprint),
              protocolVersion = initialProtocolVersion,
              expectFullAuthorization = true,
            )
            .leftMap(IdentityManagerParentError(_): ParticipantTopologyManagerError)
            .map(_ => ())
        )
      }
    } yield ()
  }
}
