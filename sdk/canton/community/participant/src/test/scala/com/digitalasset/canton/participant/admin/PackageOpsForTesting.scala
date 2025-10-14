// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.ledger.api.{
  ListVettedPackagesOpts,
  PriorTopologySerial,
  SinglePackageTargetVetting,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.PackageInUse
import com.digitalasset.canton.participant.topology.{
  PackageOps,
  ParticipantTopologyManagerError,
  ParticipantVettedPackages,
}
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.{ForceFlags, ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.ExecutionContext

class PackageOpsForTesting(
    val participantId: ParticipantId,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends PackageOps {

  override def hasVettedPackageEntry(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Boolean] =
    EitherT.rightT(false)

  override def checkPackageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageInUse, Unit] =
    EitherT.rightT(())

  override def revokeVettingForPackages(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: PackageService.DarDescription,
      psid: PhysicalSynchronizerId,
      forceFlags: ForceFlags,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    EitherT.rightT(())

  override def vetPackages(
      packages: Seq[PackageId],
      synchronizeVetting: PackageVettingSynchronization,
      psid: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    EitherT.rightT(())

  override def getVettedPackages(
      opts: ListVettedPackagesOpts
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Seq[
    ParticipantVettedPackages
  ]] =
    EitherT.rightT(Seq())

  override def updateVettedPackages(
      targetStates: Seq[SinglePackageTargetVetting[PackageId]],
      psid: PhysicalSynchronizerId,
      synchronizeVetting: PackageVettingSynchronization,
      dryRunSnapshot: Option[PackageMetadata],
      expectedTopologySerial: Option[PriorTopologySerial],
      allowUnvetPackageIdInUse: Boolean = false,
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    (Option[ParticipantVettedPackages], Option[ParticipantVettedPackages]),
  ] =
    EitherT.rightT((None, None))
}
