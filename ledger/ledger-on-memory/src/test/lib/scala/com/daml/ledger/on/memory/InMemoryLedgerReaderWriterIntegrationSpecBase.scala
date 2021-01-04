// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.kvutils.api.{
  BatchingLedgerWriterConfig,
  KeyValueParticipantState
}
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics

import scala.concurrent.duration.DurationInt

abstract class InMemoryLedgerReaderWriterIntegrationSpecBase(enableBatching: Boolean)
    extends ParticipantStateIntegrationSpecBase(
      s"In-memory ledger/participant with parallel validation ${if (enableBatching) "enabled"
      else "disabled"}") {

  private val batchingLedgerWriterConfig =
    BatchingLedgerWriterConfig(
      enableBatching = enableBatching,
      // In case of serial validation, we need a queue length of 1000 because the
      // "process many party allocations" test case will be sending in that many requests at once
      // (otherwise some of those would be rejected).
      // See [[ParticipantStateIntegrationSpecBase]].
      maxBatchQueueSize = if (enableBatching) 100 else 1000,
      maxBatchSizeBytes = 4L * 1024L * 1024L /* 4MB */,
      maxBatchWaitDuration = 100.millis,
      // In-memory ledger doesn't support concurrent commits.
      maxBatchConcurrentCommits = 1
    )

  override val isPersistent: Boolean = false

  override def participantStateFactory(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      testId: String,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): ResourceOwner[ParticipantState] =
    new InMemoryLedgerReaderWriter.SingleParticipantBatchingOwner(
      ledgerId,
      batchingLedgerWriterConfig,
      participantId,
      metrics = metrics,
      engine = Engine.DevEngine()
    ).map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter, metrics))

}
