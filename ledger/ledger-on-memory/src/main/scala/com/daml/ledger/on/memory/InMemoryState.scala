// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.util.concurrent.locks.StampedLock

import com.daml.ledger.on.memory.InMemoryState._
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.participant.state.v1.Offset
import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}

private[memory] class InMemoryState private (log: MutableLog, state: MutableState) {
  private val lockCurrentState = new StampedLock()
  @volatile private var lastLogEntryIndex = 0

  def readLog[A](action: ImmutableLog => A): A =
    action(log) // `log` is mutable, but the interface is immutable

  def newHeadSinceLastWrite(): Int = lastLogEntryIndex

  def write[A](action: (MutableLog, MutableState) => Future[A])(
      implicit executionContext: ExecutionContext
  ): Future[A] =
    for {
      stamp <- Future {
        blocking {
          lockCurrentState.writeLock()
        }
      }
      result <- action(log, state)
        .andThen {
          case _ =>
            lastLogEntryIndex = log.size - 1
            lockCurrentState.unlock(stamp)
        }
    } yield result
}

object InMemoryState {
  type ImmutableLog = IndexedSeq[LedgerRecord]
  type ImmutableState = collection.Map[StateKey, StateValue]

  type MutableLog = mutable.Buffer[LedgerRecord] with ImmutableLog
  type MutableState = mutable.Map[StateKey, StateValue] with ImmutableState

  type StateKey = Bytes
  type StateValue = Bytes

  // The first element will never be read because begin offsets are exclusive.
  private val Beginning = LedgerRecord(Offset.beforeBegin, ByteString.EMPTY, ByteString.EMPTY)

  def empty =
    new InMemoryState(
      log = mutable.ArrayBuffer(Beginning),
      state = mutable.Map.empty,
    )
}
