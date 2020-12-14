// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.metrics.{Metrics, Timed, TelemetryContext}

import scala.concurrent.Future

class TimedLedgerWriter(delegate: LedgerWriter, metrics: Metrics) extends LedgerWriter {

  override def participantId: ParticipantId =
    delegate.participantId

  override def commit(
      correlationId: String,
      envelope: Bytes,
      metadata: CommitMetadata,
  )(implicit telemetryContext: TelemetryContext): Future[SubmissionResult] =
    Timed.future(
      metrics.daml.kvutils.writer.commit,
      delegate.commit(correlationId, envelope, metadata),
    )

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()
}
