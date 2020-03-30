// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.time.Instant
import java.util.concurrent.locks.StampedLock

import com.daml.ledger.on.memory.InMemoryState._
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.kvutils.api.LedgerEntry
import com.daml.ledger.participant.state.kvutils.api.LedgerEntry.Heartbeat
import com.daml.ledger.participant.state.v1.Offset

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

private[memory] class InMemoryState(
    // the first element will never be read because begin offsets are exclusive
    log: MutableLog = mutable.ArrayBuffer(Heartbeat(Offset.begin, Instant.EPOCH)),
    state: MutableState = mutable.Map.empty,
) {
  private val lock = new StampedLock

  // The log and state only differ in the interface; they're the same objects.
  def withReadLock[A](action: (ImmutableLog, ImmutableState) => A): A = {
    val stamp = lock.readLock()
    try {
      action(log, state)
    } finally {
      lock.unlockRead(stamp)
    }
  }

  def withWriteLock[A](action: (MutableLog, MutableState) => A): A = {
    val stamp = lock.writeLock()
    try {
      action(log, state)
    } finally {
      lock.unlockWrite(stamp)
    }
  }

  def withFutureWriteLock[A](action: (MutableLog, MutableState) => Future[A])(
      implicit executionContext: ExecutionContext
  ): Future[A] = {
    val stamp = lock.writeLock()
    action(log, state)
      .andThen {
        case _ => lock.unlockWrite(stamp)
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
