// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.on.memory.InMemoryState.MutableLog
import com.daml.ledger.participant.state.kvutils.KVOffset
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.validator.BatchingLedgerStateOperations
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

private[memory] final class InMemoryLedgerStateOperations(
    val log: InMemoryState.MutableLog,
    val state: InMemoryState.MutableState,
)(implicit executionContext: ExecutionContext)
    extends BatchingLedgerStateOperations[Index] {
  import InMemoryLedgerStateOperations.appendEntry

  override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
    Future.successful(keys.map(state.get))

  override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] = {
    state ++= keyValuePairs
    Future.unit
  }

  override def appendToLog(key: Key, value: Value): Future[Index] =
    Future.successful(appendEntry(log, LedgerRecord(_, key, value)))
}

object InMemoryLedgerStateOperations {
  def apply()(implicit executionContext: ExecutionContext): InMemoryLedgerStateOperations = {
    val inMemoryState = mutable.Map.empty[Key, Value]
    val inMemoryLog = mutable.ArrayBuffer[LedgerRecord]()
    new InMemoryLedgerStateOperations(inMemoryLog, inMemoryState)
  }

  private[memory] def appendEntry(log: MutableLog, createEntry: Offset => LedgerRecord): Index = {
    val entryAtIndex = log.size
    val offset = KVOffset.fromLong(entryAtIndex.toLong)
    val entry = createEntry(offset)
    log += entry
    entryAtIndex
  }
}
