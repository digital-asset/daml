package com.daml.ledger.participant.state.kvutils.export

import java.time.Instant

import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.google.protobuf.ByteString

//TODO(miklos): Add support for storing ACLs too.
trait LedgerDataExporter {

  /**
    * Adds given submission and its parameters to the list of in-progress submissions.
    */
  def addSubmission(
      submissionEnvelope: ByteString,
      correlationId: String,
      recordTimeInstant: Instant,
      participantId: ParticipantId): Unit

  /**
    * Establishes parent-child relation between two correlation IDs.
    */
  def addChildTo(parentCorrelationId: String, childCorrelationId: String): Unit

  /**
    * Adds given key-value pairs to the write-set belonging to the given correlation ID.
    */
  def addKeyValuePairs(correlationId: String, data: Iterable[(Key, Value)]): Unit

  /**
    * Signals that entries for the given top-level (parent) correlation ID may be persisted.
    */
  def finishedEntry(correlationId: String): Unit
}

object LedgerDataExporter {
  def apply(): LedgerDataExporter = FileBasedLedgerDataExporter
}
