// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.util.concurrent.Semaphore

import com.daml.ledger.on.memory.InMemoryState._
import com.daml.ledger.participant.state.kvutils.api.LedgerEntry
import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

private[memory] class InMemoryState(
    log: Log = mutable.ArrayBuffer(),
    state: State = mutable.Map.empty,
) {
  private val lockCurrentState = new Semaphore(1, true)

  def withLock[A](action: (Log, State) => A): A = {
    lockCurrentState.acquire()
    try {
      action(log, state)
    } finally {
      lockCurrentState.release()
    }
  }

  def withFutureLock[A](action: (Log, State) => Future[A])(
      implicit executionContext: ExecutionContext
  ): Future[A] = {
    lockCurrentState.acquire()
    action(log, state)
      .andThen {
        case _ => lockCurrentState.release()
      }
  }

  def readLogEntry(index: Int): LedgerEntry =
    withLock((log, _) => log(index))
}

object InMemoryState {
  type Log = mutable.Buffer[LedgerEntry] with IndexedSeq[LedgerEntry]
  type State = mutable.Map[ByteString, Array[Byte]]
}
