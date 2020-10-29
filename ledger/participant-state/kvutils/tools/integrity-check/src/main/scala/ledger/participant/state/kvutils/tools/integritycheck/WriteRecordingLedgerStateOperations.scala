// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.kvutils.export.WriteSet
import com.daml.ledger.validator.LedgerStateOperations
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

class WriteRecordingLedgerStateOperations[LogResult](delegate: LedgerStateOperations[LogResult])
    extends LedgerStateOperations[LogResult]
    with QueryableWriteSet {
  private val recordedWriteSet = ListBuffer.empty[(Key, Value)]

  override def readState(key: Key)(
      implicit executionContext: ExecutionContext
  ): Future[Option[Value]] = delegate.readState(key)

  override def readState(keys: Iterable[Key])(
      implicit executionContext: ExecutionContext
  ): Future[Seq[Option[Value]]] =
    delegate.readState(keys)

  override def writeState(key: Key, value: Value)(
      implicit executionContext: ExecutionContext
  ): Future[Unit] = {
    this.synchronized(recordedWriteSet.append((key, value)))
    delegate.writeState(key, value)
  }

  override def writeState(keyValuePairs: Iterable[(Key, Value)])(
      implicit executionContext: ExecutionContext
  ): Future[Unit] = {
    this.synchronized(recordedWriteSet.appendAll(keyValuePairs))
    delegate.writeState(keyValuePairs)
  }

  override def appendToLog(key: Key, value: Value)(
      implicit executionContext: ExecutionContext
  ): Future[LogResult] = {
    this.synchronized(recordedWriteSet.append((key, value)))
    delegate.appendToLog(key, value)
  }

  override def getAndClearRecordedWriteSet(): WriteSet = {
    this.synchronized {
      val result = Seq(recordedWriteSet: _*)
      recordedWriteSet.clear()
      result
    }
  }
}
