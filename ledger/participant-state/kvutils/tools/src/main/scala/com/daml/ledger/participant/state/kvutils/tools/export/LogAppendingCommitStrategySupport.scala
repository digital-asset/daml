// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.export

import com.daml.ledger.on.memory.{InMemoryLedgerStateOperations, Index}
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.export.NoopLedgerDataExporter
import com.daml.ledger.participant.state.kvutils.tools.export.IntegrityChecker.bytesAsHexString
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.ledger.validator.batch.BatchedSubmissionValidatorFactory

import scala.concurrent.ExecutionContext

object LogAppendingCommitStrategySupport extends CommitStrategySupport[Index] {
  override val stateKeySerializationStrategy: StateKeySerializationStrategy =
    StateKeySerializationStrategy.createDefault()

  override def createComponentsForReplay()(
      implicit executionContext: ExecutionContext): ComponentsForReplay[Index] = {
    val inMemoryLedgerStateOperations = InMemoryLedgerStateOperations()
    val writeRecordingLedgerStateOperations =
      new WriteRecordingLedgerStateOperations[Index](inMemoryLedgerStateOperations)
    val (reader, commitStrategy) =
      BatchedSubmissionValidatorFactory.readerAndCommitStrategyFrom(
        writeRecordingLedgerStateOperations,
        stateKeySerializationStrategy,
        NoopLedgerDataExporter)
    ComponentsForReplay(reader, commitStrategy, writeRecordingLedgerStateOperations)
  }

  override def explainMismatchingValue(
      logEntryId: Key,
      expectedValue: Value,
      actualValue: Value): Option[String] = {
    val expectedLogEntry = kvutils.Envelope.openLogEntry(expectedValue)
    val actualLogEntry = kvutils.Envelope.openLogEntry(actualValue)
    Some(
      s"Log entry ID: ${bytesAsHexString(logEntryId)}${System.lineSeparator()}" +
        s"Expected: $expectedLogEntry${System.lineSeparator()}Actual: $actualLogEntry"
    )
  }
}
