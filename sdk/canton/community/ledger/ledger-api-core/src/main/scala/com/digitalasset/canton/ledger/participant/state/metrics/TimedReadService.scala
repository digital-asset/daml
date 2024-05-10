// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.metrics

import com.daml.metrics.Timed
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.participant.state.{InternalStateService, ReadService, Update}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.tracing.{TraceContext, Traced}
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
}
