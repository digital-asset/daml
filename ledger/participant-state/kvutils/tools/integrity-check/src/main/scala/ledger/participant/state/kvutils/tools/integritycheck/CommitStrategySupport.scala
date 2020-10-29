// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.kvutils.export.WriteSet
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

final case class ComponentsForReplay[LogResult](
    ledgerStateReader: DamlLedgerStateReader,
    commitStrategy: CommitStrategy[LogResult],
    queryableWriteSet: QueryableWriteSet,
)

trait CommitStrategySupport[LogResult] {
  def stateKeySerializationStrategy(): StateKeySerializationStrategy

  def createComponentsForReplay()(
      implicit executionContext: ExecutionContext): ComponentsForReplay[LogResult]

  /**
    * Determines if there's an actual difference and tries to explain it.
    *
    * @return None in case no difference should be signaled to the user; otherwise a message explaining the difference
    */
  def explainMismatchingValue(key: Key, expectedValue: Value, actualValue: Value): Option[String]
}
