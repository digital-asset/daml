// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.PackageInUse
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class PackageOpsForTesting(
    val participantId: ParticipantId,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends PackageOps {

  override def isPackageVetted(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, CantonError, Boolean] =
    EitherT.rightT(false)

  override def checkPackageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageInUse, Unit] =
    EitherT.rightT(())

  override def revokeVettingForPackages(
      mainPkg: LfPackageId,
      packages: List[LfPackageId],
      darDescriptor: PackageService.DarDescriptor,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, CantonError, Unit] =
    EitherT.rightT(())

  override def vetPackages(packages: Seq[PackageId], synchronize: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    EitherT.rightT(())
}
