// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.time.Instant
import java.util.concurrent.Semaphore

import com.daml.ledger.on.memory.InMemoryState._
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.kvutils.api.LedgerEntry
import com.daml.ledger.participant.state.kvutils.api.LedgerEntry.Heartbeat
import com.daml.ledger.participant.state.v1.Offset

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

private[memory] class InMemoryState(
    // the first element will never be read because begin offsets are exclusive
    log: MutableLog = mutable.ArrayBuffer(Heartbeat(Offset.empty, Instant.EPOCH)),
    state: MutableState = mutable.Map.empty,
) {
  private val lockCurrentState = new Semaphore(1, true)

  // This only differs in the interface; it uses the same lock and provides the same objects.
  def withReadLock[A](action: (ImmutableLog, ImmutableState) => A): A =
    withWriteLock(action)

  def withWriteLock[A](action: (MutableLog, MutableState) => A): A = {
    lockCurrentState.acquire()
    try {
      action(log, state)
    } finally {
      lockCurrentState.release()
    }
  }

  def withFutureWriteLock[A](action: (MutableLog, MutableState) => Future[A])(
      implicit executionContext: ExecutionContext
  ): Future[A] = {
    lockCurrentState.acquire()
    action(log, state)
      .andThen {
        case _ => lockCurrentState.release()
      }
  }
}

object InMemoryState {
  type ImmutableLog = IndexedSeq[LedgerEntry]
  type ImmutableState = collection.Map[StateKey, StateValue]

  type MutableLog = mutable.Buffer[LedgerEntry] with ImmutableLog
  type MutableState = mutable.Map[StateKey, StateValue] with ImmutableState

  type StateKey = Bytes
  type StateValue = Bytes
}
