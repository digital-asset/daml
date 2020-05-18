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

class InMemoryBatchedLedgerReaderWriterIntegrationSpec
    extends ParticipantStateIntegrationSpecBase(
      "In-memory ledger/participant with parallel validation") {

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
        BatchingQueueFactory.batchingQueueFrom(BatchingLedgerWriterConfig.reasonableDefault),
        readerWriter)
      new KeyValueParticipantState(readerWriter, batchingLedgerWriter, metrics)
    }
  }
}
