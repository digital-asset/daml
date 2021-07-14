// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.inmemory

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import org.slf4j.LoggerFactory
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.sandbox.stores.ledger.SandboxOffset

import scala.collection.compat._
import scala.collection.immutable.TreeMap

private[inmemory] class LedgerEntries[T](identify: T => String) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private case class Entries(ledgerEnd: Offset, items: TreeMap[Offset, T])

  // Tuple of (ledger end cursor, ledger map). There is never an entry for the initial cursor. End is inclusive.
  private val state = new AtomicReference(Entries(ledgerBeginning, TreeMap.empty))

  private def store(item: T): Offset = {
    val Entries(newOffset, _) = state.updateAndGet({ case Entries(ledgerEnd, ledger) =>
      val newEnd = SandboxOffset.toOffset(SandboxOffset.fromOffset(ledgerEnd) + 1)
      Entries(newEnd, ledger + (newEnd -> item))
    })
    if (logger.isTraceEnabled())
      logger.trace("Recording `{}` at offset `{}`", identify(item): Any, newOffset.toApiString: Any)
    newOffset
  }

  def incrementOffset(increment: Int): Offset = {
    val Entries(newOffset, _) = state.updateAndGet({ case Entries(ledgerEnd, ledger) =>
      val newEnd = SandboxOffset.toOffset(SandboxOffset.fromOffset(ledgerEnd) + increment)
      Entries(newEnd, ledger)
    })
    if (logger.isTraceEnabled())
      logger.trace("Bumping offset to `{}`", newOffset.toApiString)
    newOffset
  }

  private val dispatcher = Dispatcher[Offset]("inmemory-ledger", Offset.beforeBegin, ledgerEnd)

  def getSource(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
  ): Source[(Offset, T), NotUsed] =
    dispatcher.startingAt(
      startExclusive.getOrElse(ledgerBeginning),
      RangeSource((exclusiveStart, inclusiveEnd) =>
        Source[(Offset, T)](
          state
            .get()
            .items
            .rangeFrom(exclusiveStart)
            .filter(_._1 > exclusiveStart)
            .rangeTo(inclusiveEnd)
        )
      ),
      endInclusive,
    )

  def publish(item: T): Offset = {
    val newHead = store(item)
    dispatcher.signalNewHead(newHead)
    newHead
  }

  def ledgerBeginning: Offset = SandboxOffset.toOffset(0)

  def items = state.get().items.iterator

  def ledgerEnd: Offset = state.get().ledgerEnd

  def nextTransactionId: Ref.LedgerString =
    Ref.LedgerString.assertFromString((SandboxOffset.fromOffset(ledgerEnd) + 1).toString)
}
