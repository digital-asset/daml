package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.kvutils.api.{
  BatchingLedgerWriter,
  KeyValueParticipantState
}
import com.daml.ledger.participant.state.kvutils.app.batch.{
  BatchingLedgerWriterConfig,
  BatchingQueueFactory
}
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.resources.ResourceOwner

import scala.concurrent.duration._

class InMemoryBatchedLedgerReaderWriterIntegrationSpec
    extends ParticipantStateIntegrationSpecBase(
      "In-memory ledger/participant with parallel validation") {
  private val enableBatching = true
  private val batchingLedgerWriterConfig =
    BatchingLedgerWriterConfig(
      enableBatching = enableBatching,
      // In case of serial validation, we need a queue length of 1000 because the
      // "process many party allocations" test case will be sending in that many requests at once
      // (otherwise some of those would be rejected).
      // See [[ParticipantStateIntegrationSpecBase]].
      maxBatchQueueSize = if (enableBatching) 100 else 1000,
      maxBatchSizeBytes = 4 * 1024 * 1024 /* 4MB */,
      maxBatchWaitDuration = 100.millis,
      // In-memory ledger doesn't support concurrent commits.
      maxBatchConcurrentCommits = 1
    )

  override val isPersistent: Boolean = false

  override def participantStateFactory(
      ledgerId: Option[LedgerId],
      participantId: ParticipantId,
      testId: String,
      metrics: Metrics,
  )(implicit logCtx: LoggingContext): ResourceOwner[ParticipantState] = {
    new InMemoryBatchedLedgerReaderWriter.SingleParticipantOwner(
      ledgerId,
      participantId,
      metrics = metrics,
      engine = Engine(),
    ).map { readerWriter =>
      val batchingLedgerWriter = new BatchingLedgerWriter(
        BatchingQueueFactory.batchingQueueFrom(batchingLedgerWriterConfig),
        readerWriter)
      new KeyValueParticipantState(readerWriter, batchingLedgerWriter, metrics)
    }
  }
}
