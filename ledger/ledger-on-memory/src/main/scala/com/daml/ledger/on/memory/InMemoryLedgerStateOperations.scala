// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.on.memory.InMemoryState.MutableLog
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.participant.state.kvutils.{OffsetBuilder, Raw}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.validator.BatchingLedgerStateOperations

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

private[memory] final class InMemoryLedgerStateOperations(
    log: InMemoryState.MutableLog,
    state: InMemoryState.MutableState,
) extends BatchingLedgerStateOperations[Index] {

  import InMemoryLedgerStateOperations.appendEntry

  override def readState(
      keys: Iterable[Raw.Key]
  )(implicit executionContext: ExecutionContext): Future[Seq[Option[Raw.Value]]] =
    Future.successful(keys.view.map(state.get).toSeq)

  override def writeState(
      keyValuePairs: Iterable[Raw.KeyValuePair]
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    state ++= keyValuePairs
    Future.unit
  }

  override def appendToLog(key: Raw.Key, value: Raw.Value)(implicit
      executionContext: ExecutionContext
  ): Future[Index] =
    Future.successful(appendEntry(log, LedgerRecord(_, key, value)))
}

object InMemoryLedgerStateOperations {
  def apply(): InMemoryLedgerStateOperations = {
    val inMemoryState = mutable.Map.empty[Raw.Key, Raw.Value]
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
