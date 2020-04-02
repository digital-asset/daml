// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.util.concurrent.Semaphore

import com.daml.ledger.on.memory.InMemoryState._
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.kvutils.api.LedgerEntry
import com.daml.ledger.participant.state.kvutils.api.LedgerEntry.LedgerRecord
import com.daml.ledger.participant.state.v1.Offset
import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

private[memory] class InMemoryState(
    // the first element will never be read because begin offsets are exclusive
    log: MutableLog =
      mutable.ArrayBuffer(LedgerRecord(Offset.begin, ByteString.EMPTY, ByteString.EMPTY)),
    state: MutableState = mutable.Map.empty,
) {
  private val lockCurrentState = new Semaphore(1, true)

  def readLog[A](action: ImmutableLog => A): A =
    action(log) // `log` is mutable, but the interface is immutable

  def write[A](action: (MutableLog, MutableState) => Future[A])(
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
