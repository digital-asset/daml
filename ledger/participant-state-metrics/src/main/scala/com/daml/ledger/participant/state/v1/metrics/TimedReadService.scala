// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1.metrics

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.v1.{LedgerInitialConditions, Offset, ReadService, UpdateWithContext}
import com.daml.metrics.{Metrics, Timed}

final class TimedReadService(delegate: ReadService, metrics: Metrics) extends ReadService {

  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Timed.source(
      metrics.daml.services.read.getLedgerInitialConditions,
      delegate.getLedgerInitialConditions())

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, UpdateWithContext), NotUsed] =
    Timed.source(metrics.daml.services.read.stateUpdates, delegate.stateUpdates(beginAfter))

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()
}
