// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2.metrics

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.configuration.LedgerInitialConditions
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.{ReadService, Update}
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}

final class TimedReadService(delegate: ReadService, metrics: Metrics) extends ReadService {

  override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Timed.source(
      metrics.daml.services.read.getLedgerInitialConditions,
      delegate.ledgerInitialConditions(),
    )

  override def stateUpdates(
      beginAfter: Option[Offset]
  )(implicit loggingContext: LoggingContext): Source[(Offset, Update), NotUsed] =
    Timed.source(metrics.daml.services.read.stateUpdates, delegate.stateUpdates(beginAfter))

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()
}
