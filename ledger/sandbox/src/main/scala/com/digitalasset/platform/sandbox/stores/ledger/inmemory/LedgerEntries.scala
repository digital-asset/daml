// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.inmemory

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.RangeSource
import org.slf4j.LoggerFactory

import scala.collection.immutable.TreeMap

private[ledger] class LedgerEntries[T](identify: T => String) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private case class Entries(ledgerEnd: Long, items: TreeMap[Long, T])

  // Tuple of (ledger end cursor, ledger map). There is never an entry for the initial cursor. End is inclusive.
  private val state = new AtomicReference(Entries(ledgerBeginning, TreeMap.empty))

  private def store(item: T): Long = {
    val Entries(newOffset, _) = state.updateAndGet({
      case Entries(ledgerEnd, ledger) => Entries(ledgerEnd + 1, ledger + (ledgerEnd -> item))
    })
    if (logger.isTraceEnabled())
      logger.trace("Recording `{}` at offset `{}`", identify(item), newOffset)
    newOffset
  }

  private val dispatcher = Dispatcher[Long, T](
    RangeSource(
      (inclusiveStart, exclusiveEnd) =>
        Source[(Long, T)](state.get().items.range(inclusiveStart, exclusiveEnd)),
    ),
    ledgerBeginning,
    ledgerEnd
  )

  def getSource(offset: Option[Long]): Source[(Long, T), NotUsed] =
    dispatcher.startingAt(offset.getOrElse(ledgerBeginning))

  def publish(item: T): Long = {
    val newHead = store(item)
    dispatcher.signalNewHead(newHead)
    newHead
  }

  def ledgerBeginning: Long = 0L

  def ledgerEnd: Long = state.get().ledgerEnd

  def getEntryAt(offset: Long): Option[T] =
    state.get.items.get(offset)
}
