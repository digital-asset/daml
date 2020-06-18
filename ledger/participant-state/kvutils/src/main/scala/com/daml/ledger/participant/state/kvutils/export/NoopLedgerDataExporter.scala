package com.daml.ledger.participant.state.kvutils.export

import java.time.Instant

import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.google.protobuf.ByteString

object NoopLedgerDataExporter extends LedgerDataExporter {
  override def addSubmission(
      submissionEnvelope: ByteString,
      correlationId: String,
      recordTimeInstant: Instant,
      participantId: ParticipantId): Unit = ()

  override def addChildTo(parentCorrelationId: String, childCorrelationId: String): Unit = ()

  override def addKeyValuePairs(correlationId: String, data: Iterable[(Key, Value)]): Unit = ()

  override def finishedEntry(correlationId: String): Unit = ()
}
