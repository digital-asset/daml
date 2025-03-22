// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.digitalasset.base.error.CantonRpcError
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.PackageInUse
import com.digitalasset.canton.participant.topology.{PackageOps, ParticipantTopologyManagerError}
import com.digitalasset.canton.topology.ParticipantId
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
  ): EitherT[FutureUnlessShutdown, CantonRpcError, Boolean] =
    EitherT.rightT(false)

  override def checkPackageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageInUse, Unit] =
    EitherT.rightT(())

  override def revokeVettingForPackages(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: PackageService.DarDescription,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, CantonRpcError, Unit] =
    EitherT.rightT(())

  override def vetPackages(
      packages: Seq[PackageId],
      synchronizeVetting: PackageVettingSynchronization,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    EitherT.rightT(())
}
