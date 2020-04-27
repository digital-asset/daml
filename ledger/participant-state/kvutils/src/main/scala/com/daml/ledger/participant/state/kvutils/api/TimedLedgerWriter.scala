// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.metrics.Timed

import scala.concurrent.Future

class TimedLedgerWriter(delegate: LedgerWriter, metricRegistry: MetricRegistry)
    extends LedgerWriter {

  override def participantId: ParticipantId =
    delegate.participantId

  override def commit(correlationId: String, envelope: Bytes): Future[SubmissionResult] =
    Timed.future(Metrics.commit, delegate.commit(correlationId, envelope))

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()

  private object Metrics {
    private val Prefix = kvutils.MetricPrefix :+ "writer"

    val commit: Timer = metricRegistry.timer(Prefix :+ "commit")
  }

}
