// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.codahale.metrics.Timer
import com.daml.ledger.offset.Offset
import com.daml.platform.store.cache.MutableCacheBackedContractStore.SignalNewLedgerHead
import com.daml.scalautil.Statement.discard

import java.util.concurrent.TimeUnit
import scala.collection.mutable

/** Computes the lag between the contract state events dispatcher and the general dispatcher.
  *
  * Internally uses a size bound for preventing memory leaks if misused.
  *
  * @param delegate The ledger head dispatcher delegate.
  * @param timer The timer measuring the delta.
  */
private[index] class InstrumentedSignalNewLedgerHead(
    delegate: SignalNewLedgerHead,
    maxSize: Long = 1000L,
)(
    timer: Timer
) extends SignalNewLedgerHead {
  private val ledgerHeads = mutable.Map.empty[Offset, Long]

  override def apply(offset: Offset, sequentialEventId: Long): Unit = {
    delegate(offset, sequentialEventId)
    ledgerHeads.synchronized {
      ledgerHeads.remove(offset).foreach { startNanos =>
        val endNanos = System.nanoTime()
        timer.update(endNanos - startNanos, TimeUnit.NANOSECONDS)
      }
    }
  }

  private[index] def startTimer(head: Offset): Unit =
    ledgerHeads.synchronized {
      ensureBounded()
      discard(ledgerHeads.getOrElseUpdate(head, System.nanoTime()))
    }

  private def ensureBounded(): Unit =
    if (ledgerHeads.size > maxSize) {
      // If maxSize is reached, remove randomly ANY element.
      ledgerHeads.headOption.foreach(head => ledgerHeads.remove(head._1))
    } else ()
}
