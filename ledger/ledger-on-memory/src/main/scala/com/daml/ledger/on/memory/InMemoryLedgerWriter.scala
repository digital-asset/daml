// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.api.util.TimeProvider
import com.daml.caching.Cache
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.api.{CommitMetadata, LedgerWriter}
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.validator.ValidateAndCommit
import com.daml.platform.akkastreams.dispatcher.Dispatcher

import scala.concurrent.Future
import scala.util.Success

final class InMemoryLedgerWriter private[memory] (
    override val participantId: ParticipantId,
    dispatcher: Dispatcher[Index],
    state: InMemoryState,
    validateAndCommit: ValidateAndCommit,
) extends LedgerWriter {
  override def commit(
      correlationId: String,
      envelope: Raw.Value,
      metadata: CommitMetadata,
  ): Future[SubmissionResult] =
    validateAndCommit(correlationId, envelope, participantId)
      .andThen { case Success(SubmissionResult.Acknowledged) =>
        dispatcher.signalNewHead(state.newHeadSinceLastWrite())
      }(DirectExecutionContext)

  override def currentHealth(): HealthStatus = Healthy
}

object InMemoryLedgerWriter {

  private[memory] val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  private[memory] type StateValueCache = Cache[DamlStateKey, DamlStateValue]

}
