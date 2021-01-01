// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.on.memory.InMemoryState.MutableLog
import com.daml.ledger.participant.state.kvutils.OffsetBuilder
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.validator.BatchingLedgerStateOperations
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.collection.{breakOut, mutable}
import scala.concurrent.{ExecutionContext, Future}

private[memory] final class InMemoryLedgerStateOperations(
    log: InMemoryState.MutableLog,
    state: InMemoryState.MutableState,
) extends BatchingLedgerStateOperations[Index] {

  import InMemoryLedgerStateOperations.appendEntry

  override def readState(
      keys: Iterable[Key],
  )(implicit executionContext: ExecutionContext): Future[Seq[Option[Value]]] =
    Future.successful(keys.map(state.get)(breakOut))

  override def writeState(
      keyValuePairs: Iterable[(Key, Value)],
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    state ++= keyValuePairs
    Future.unit
  }

  override def appendToLog(key: Key, value: Value)(
      implicit executionContext: ExecutionContext
  ): Future[Index] =
    Future.successful(appendEntry(log, LedgerRecord(_, key, value)))
}

object InMemoryLedgerStateOperations {
  def apply(): InMemoryLedgerStateOperations = {
    val inMemoryState = mutable.Map.empty[Key, Value]
    val inMemoryLog = mutable.ArrayBuffer[LedgerRecord]()
    new InMemoryLedgerStateOperations(inMemoryLog, inMemoryState)
  }

  private[memory] def appendEntry(log: MutableLog, createEntry: Offset => LedgerRecord): Index = {
    val entryAtIndex = log.size
    val offset = OffsetBuilder.fromLong(entryAtIndex.toLong)
    val entry = createEntry(offset)
    log += entry
    entryAtIndex
  }
}
