// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.v2.metrics

import com.daml.metrics.Timed
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.{ReadService, SubmissionResult, Update}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

final class TimedReadService(delegate: ReadService, metrics: Metrics) extends ReadService {

  override def stateUpdates(
      beginAfter: Option[Offset]
  )(implicit traceContext: TraceContext): Source[(Offset, Traced[Update]), NotUsed] =
    Timed.source(metrics.daml.services.read.stateUpdates, delegate.stateUpdates(beginAfter))

  override def getConnectedDomains(
      request: ReadService.ConnectedDomainRequest
  )(implicit traceContext: TraceContext): Future[ReadService.ConnectedDomainResponse] =
    Timed.future(
      metrics.daml.services.read.getConnectedDomains,
      delegate.getConnectedDomains(request),
    )

  override def validateDar(dar: ByteString)(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult] =
    Timed.future(
      metrics.daml.services.read.validateDar,
      delegate.validateDar(dar),
    )

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()
}
