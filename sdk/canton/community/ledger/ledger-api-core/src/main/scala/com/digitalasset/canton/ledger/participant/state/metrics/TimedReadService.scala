// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.metrics

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.ContextualizedErrorLogger
import com.daml.lf.data.Ref.PackageId
import com.daml.metrics.Timed
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.participant.state.index.PackageDetails
import com.digitalasset.canton.ledger.participant.state.{
  InternalStateService,
  ReadService,
  SubmissionResult,
  Update,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

final class TimedReadService(delegate: ReadService, metrics: LedgerApiServerMetrics)
    extends ReadService {

  override def stateUpdates(
      beginAfter: Option[Offset]
  )(implicit traceContext: TraceContext): Source[(Offset, Traced[Update]), NotUsed] =
    Timed.source(metrics.services.read.stateUpdates, delegate.stateUpdates(beginAfter))

  override def getConnectedDomains(
      request: ReadService.ConnectedDomainRequest
  )(implicit traceContext: TraceContext): Future[ReadService.ConnectedDomainResponse] =
    Timed.future(
      metrics.services.read.getConnectedDomains,
      delegate.getConnectedDomains(request),
    )

  override def incompleteReassignmentOffsets(validAt: Offset, stakeholders: Set[LfPartyId])(implicit
      traceContext: TraceContext
  ): Future[Vector[Offset]] =
    Timed.future(
      metrics.services.read.getConnectedDomains,
      delegate.incompleteReassignmentOffsets(validAt, stakeholders),
    )

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()

  override def registerInternalStateService(internalStateService: InternalStateService): Unit =
    delegate.registerInternalStateService(internalStateService)

  override def internalStateService: Option[InternalStateService] =
    delegate.internalStateService

  override def unregisterInternalStateService(): Unit =
    delegate.unregisterInternalStateService()

  override def getPackageMetadataSnapshot(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PackageMetadata =
    delegate.getPackageMetadataSnapshot

  override def listLfPackages()(implicit
      traceContext: TraceContext
  ): Future[Map[PackageId, PackageDetails]] =
    Timed.future(
      metrics.services.read.listLfPackages,
      delegate.listLfPackages(),
    )

  override def getLfArchive(
      packageId: PackageId
  )(implicit traceContext: TraceContext): Future[Option[Archive]] =
    Timed.future(
      metrics.services.read.getLfArchive,
      delegate.getLfArchive(packageId),
    )

  override def validateDar(dar: ByteString, darName: String)(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult] =
    Timed.future(
      metrics.services.read.validateDar,
      delegate.validateDar(dar, darName),
    )
}
