// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.offset.Offset
import com.daml.ledger.on.memory.InMemoryState.MutableLog
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.participant.state.kvutils.{Raw, VersionedOffsetBuilder}
import com.daml.ledger.validator.BatchingLedgerStateOperations
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

final class InMemoryLedgerStateOperations(
    offsetBuilder: VersionedOffsetBuilder,
    log: InMemoryState.MutableLog,
    state: InMemoryState.MutableState,
) extends BatchingLedgerStateOperations[Index] {
  override def readState(
      keys: Iterable[Raw.StateKey]
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Seq[Option[Raw.Envelope]]] =
    Future.successful(keys.view.map(state.get).toSeq)

  override def writeState(
      keyValuePairs: Iterable[Raw.StateEntry]
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] = {
    state ++= keyValuePairs
    Future.unit
  }

  override def appendToLog(
      key: Raw.LogEntryId,
      value: Raw.Envelope,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Index] =
    Future.successful(appendEntry(log, LedgerRecord(_, key, value)))

  private def appendEntry(log: MutableLog, createEntry: Offset => LedgerRecord): Index = {
    val entryAtIndex = log.size
    val offset = offsetBuilder.of(entryAtIndex.toLong)
    val entry = createEntry(offset)
    log += entry
    entryAtIndex
  }
}
