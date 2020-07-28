// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools

import com.daml.ledger.participant.state.kvutils.export.FileBasedLedgerDataExporter.WriteSet
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.{
  CommitStrategy,
  DamlLedgerStateReader,
  StateKeySerializationStrategy
}

import scala.concurrent.ExecutionContext

trait QueryableWriteSet {
  def getAndClearRecordedWriteSet(): WriteSet
}

trait CommitStrategySupport[LogResult] {
  def stateKeySerializationStrategy(): StateKeySerializationStrategy

  def createComponents()(implicit executionContext: ExecutionContext)
    : (DamlLedgerStateReader, CommitStrategy[LogResult], QueryableWriteSet)

  def explainMismatchingValue(key: Key, expectedValue: Value, actualValue: Value): String
}
