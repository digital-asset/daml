// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.stream.Materializer
import com.daml.ledger.on.memory.{InMemoryLedgerStateOperations, Index}
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.export.{
  NoOpLedgerDataExporter,
  SubmissionInfo,
  WriteSet,
}
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.IntegrityChecker.rawHexString
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueCommitting, Raw}
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.ledger.validator.batch.{
  BatchedSubmissionValidator,
  BatchedSubmissionValidatorFactory,
  BatchedSubmissionValidatorParameters,
  ConflictDetection,
}
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics

import scala.concurrent.{ExecutionContext, Future}

final class LogAppendingCommitStrategySupport(
    metrics: Metrics
)(implicit executionContext: ExecutionContext)
    extends CommitStrategySupport[Index] {
  private val ledgerStateOperations =
    new WriteRecordingLedgerStateOperations[Index](InMemoryLedgerStateOperations())

  private val serializationStrategy = StateKeySerializationStrategy.createDefault()

  private val (ledgerStateReader, commitStrategy) =
    BatchedSubmissionValidatorFactory.readerAndCommitStrategyFrom(
      ledgerStateOperations,
      serializationStrategy,
    )

  private val engine = new Engine()

  private val submissionValidator = BatchedSubmissionValidator[Index](
    params = BatchedSubmissionValidatorParameters(cpuParallelism = 1, readParallelism = 1),
    committer = new KeyValueCommitting(engine, metrics),
    conflictDetection = new ConflictDetection(metrics),
    metrics = metrics,
    ledgerDataExporter = NoOpLedgerDataExporter,
  )

  override val stateKeySerializationStrategy: StateKeySerializationStrategy =
    serializationStrategy

  override def commit(
      submissionInfo: SubmissionInfo
  )(implicit materializer: Materializer): Future[WriteSet] =
    submissionValidator
      .validateAndCommit(
        submissionInfo.submissionEnvelope,
        submissionInfo.correlationId,
        submissionInfo.recordTimeInstant,
        submissionInfo.participantId,
        ledgerStateReader,
        commitStrategy,
      )
      .map(_ => ledgerStateOperations.getAndClearRecordedWriteSet())

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
