// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.inmemory

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.RangeSource
import org.slf4j.LoggerFactory

import scala.collection.immutable.TreeMap

private[ledger] class LedgerEntries[T](identify: T => String) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private case class Entries(ledgerEnd: Offset, items: TreeMap[Offset, T])

  // Tuple of (ledger end cursor, ledger map). There is never an entry for the initial cursor. End is inclusive.
  private val state = new AtomicReference(Entries(ledgerBeginning, TreeMap.empty))

  private def store(item: T): Offset = {
    val Entries(newOffset, _) = state.updateAndGet({
      case Entries(ledgerEnd, ledger) =>
        val newEnd = Offset.fromLong(ledgerEnd.value.toLong + 1)
        Entries(newEnd, ledger + (newEnd -> item))
    })
    if (logger.isTraceEnabled())
      logger.trace("Recording `{}` at offset `{}`", identify(item): Any, newOffset: Any)
    newOffset
  }

  def incrementOffset(increment: Int): Offset = {
    val Entries(newOffset, _) = state.updateAndGet({
      case Entries(ledgerEnd, ledger) =>
        val newEnd = Offset.fromLong(ledgerEnd.value.toLong + increment)
        Entries(newEnd, ledger)
    })
    if (logger.isTraceEnabled())
      logger.trace("Bumping offset to `{}`", newOffset)
    newOffset
  }

  private val dispatcher = Dispatcher[Offset]("inmemory-ledger", Offset.empty, ledgerEnd)

  def getSource(
      beginExclusive: Option[Offset],
      endInclusive: Option[Offset]): Source[(Offset, T), NotUsed] =
    dispatcher.startingAt(
      beginExclusive.getOrElse(ledgerBeginning),
      RangeSource(
        (exclusiveStart, inclusiveEnd) =>
          Source[(Offset, T)](
            state.get().items.from(exclusiveStart).filter(_._1 > exclusiveStart).to(inclusiveEnd)),
      ),
      endInclusive
    )

  def publish(item: T): Offset = {
    val newHead = store(item)
    dispatcher.signalNewHead(newHead)
    newHead
  }

  def ledgerBeginning: Offset = Offset.fromLong(0)

  def items = state.get().items.iterator

  def ledgerEnd: Offset = state.get().ledgerEnd

  def nextTransactionId: Ref.LedgerString =
    Ref.LedgerString.fromLong(ledgerEnd.value.toLong + 1)
}
