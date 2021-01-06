// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.export.{WriteItem, WriteSet}
import com.daml.ledger.validator.LedgerStateOperations

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

class WriteRecordingLedgerStateOperations[LogResult](delegate: LedgerStateOperations[LogResult])
    extends LedgerStateOperations[LogResult]
    with QueryableWriteSet {
  private val recordedWriteSet = ListBuffer.empty[WriteItem]

  override def readState(
      key: Raw.Key
  )(implicit executionContext: ExecutionContext): Future[Option[Raw.Value]] =
    delegate.readState(key)

  override def readState(
      keys: Iterable[Raw.Key]
  )(implicit executionContext: ExecutionContext): Future[Seq[Option[Raw.Value]]] =
    delegate.readState(keys)

  override def writeState(
      key: Raw.Key,
      value: Raw.Value,
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    this.synchronized(recordedWriteSet.append((key, value)))
    delegate.writeState(key, value)
  }

  override def writeState(
      keyValuePairs: Iterable[Raw.Pair]
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    this.synchronized(recordedWriteSet.appendAll(keyValuePairs))
    delegate.writeState(keyValuePairs)
  }

  override def appendToLog(
      key: Raw.Key,
      value: Raw.Value,
  )(implicit executionContext: ExecutionContext): Future[LogResult] = {
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
