// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1.metrics

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.metrics.Metrics
import com.daml.ledger.participant.state.v1.{LedgerInitialConditions, Offset, ReadService, Update}
import com.digitalasset.ledger.api.health.HealthStatus

final class TimedReadService(delegate: ReadService, metrics: MetricRegistry, prefix: String)
    extends ReadService {
  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    time("getLedgerInitialConditions", delegate.getLedgerInitialConditions())

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
    time("stateUpdates", delegate.stateUpdates(beginAfter))

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()

  private def time[Out, Mat](name: String, source: => Source[Out, Mat]): Source[Out, Mat] =
    Metrics.timedSource(metrics.timer(s"$prefix.$name"), source)
}
