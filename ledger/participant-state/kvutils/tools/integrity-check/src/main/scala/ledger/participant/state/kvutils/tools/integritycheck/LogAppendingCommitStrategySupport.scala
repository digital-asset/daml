// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.on.memory.{InMemoryLedgerStateOperations, Index}
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.IntegrityChecker.rawHexString
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}
import com.daml.ledger.validator.batch.BatchedSubmissionValidatorFactory
import com.daml.ledger.validator.reading.DamlLedgerStateReader
import com.daml.ledger.validator.{CommitStrategy, StateKeySerializationStrategy}
import com.daml.metrics.Metrics

import scala.concurrent.ExecutionContext

final class LogAppendingCommitStrategySupport(implicit executionContext: ExecutionContext)
    extends CommitStrategySupport[Index] {
  private val metrics = new Metrics(new MetricRegistry)

  private val ledgerStateOperations =
    InMemoryLedgerStateOperations()

  private val writeRecordingLedgerStateOperations =
    new WriteRecordingLedgerStateOperations[Index](ledgerStateOperations)

  private val serializationStrategy = StateKeySerializationStrategy.createDefault()

  private val readerAndCommitStrategy =
    BatchedSubmissionValidatorFactory.readerAndCommitStrategyFrom(
      writeRecordingLedgerStateOperations,
      serializationStrategy,
    )

  override val stateKeySerializationStrategy: StateKeySerializationStrategy =
    serializationStrategy

  override val writeSet: QueryableWriteSet = writeRecordingLedgerStateOperations

  override val ledgerStateReader: DamlLedgerStateReader = readerAndCommitStrategy._1

  override val commitStrategy: CommitStrategy[Index] =
    readerAndCommitStrategy._2

  override def newReadServiceFactory(): ReplayingReadServiceFactory =
    new LogAppendingReadServiceFactory(metrics)

  override def explainMismatchingValue(
      logEntryId: Raw.Key,
      expectedValue: Raw.Value,
      actualValue: Raw.Value,
  ): Option[String] = {
    val expectedLogEntry = kvutils.Envelope.openLogEntry(expectedValue)
    val actualLogEntry = kvutils.Envelope.openLogEntry(actualValue)
    Some(
      s"Log entry ID: ${rawHexString(logEntryId)}${System.lineSeparator()}" +
        s"Expected: $expectedLogEntry${System.lineSeparator()}Actual: $actualLogEntry"
    )
  }

  override def checkEntryIsReadable(rawKey: Raw.Key, rawValue: Raw.Value): Either[String, Unit] =
    Envelope.open(rawValue) match {
      case Left(errorMessage) =>
        Left(s"Invalid value envelope: $errorMessage")
      case Right(Envelope.LogEntryMessage(logEntry)) =>
        val _ = DamlLogEntryId.parseFrom(rawKey.bytes)
        if (logEntry.getPayloadCase == DamlLogEntry.PayloadCase.PAYLOAD_NOT_SET)
          Left("Log entry payload not set.")
        else
          Right(())
      case Right(Envelope.StateValueMessage(value)) =>
        val key = stateKeySerializationStrategy.deserializeStateKey(rawKey)
        if (key.getKeyCase == DamlStateKey.KeyCase.KEY_NOT_SET)
          Left("State key not set.")
        else if (value.getValueCase == DamlStateValue.ValueCase.VALUE_NOT_SET)
          Left("State value not set.")
        else
          Right(())
      case Right(Envelope.SubmissionMessage(submission)) =>
        Left(s"Unexpected submission message: $submission")
      case Right(Envelope.SubmissionBatchMessage(batch)) =>
        Left(s"Unexpected submission batch message: $batch")
    }
}
