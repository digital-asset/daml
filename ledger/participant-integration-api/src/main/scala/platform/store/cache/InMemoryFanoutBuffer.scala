// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.ledger.offset.Offset
import com.daml.logging.ContextualizedLogger
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.cache.InMemoryFanoutBuffer._
import com.daml.platform.store.interfaces.TransactionLogUpdate

import scala.collection.Searching.{Found, InsertionPoint, SearchResult}
import scala.collection.View

/** The in-memory fan-out buffer.
  *
  * This buffer stores the last ingested `maxBufferSize` accepted and rejected submission updates
  * as [[TransactionLogUpdate]] and allows bypassing IndexDB persistence fetches for recent updates for:
  *   - flat and transaction tree streams
  *   - command completion streams
  *   - by-event-id and by-transaction-id flat and transaction tree lookups
  *
  * @param maxBufferSize The maximum buffer size.
  * @param metrics The Daml metrics.
  * @param maxBufferedChunkSize The maximum size of buffered chunks returned by `slice`.
  */
class InMemoryFanoutBuffer(
    maxBufferSize: Int,
    metrics: Metrics,
    maxBufferedChunkSize: Int,
) {
  private val logger = ContextualizedLogger.get(getClass)
  @volatile private[cache] var _bufferLog =
    Vector.empty[(Offset, TransactionLogUpdate)]
  @volatile private[cache] var _lookupMap =
    Map.empty[TransactionId, TransactionLogUpdate.TransactionAccepted]

  private val bufferMetrics = metrics.daml.services.index.InMemoryFanoutBuffer
  private val pushTimer = bufferMetrics.push
  private val pruneTimer = bufferMetrics.prune
  private val bufferSizeHistogram = bufferMetrics.bufferSize

  /** Appends a new event to the buffer.
    *
    * Starts evicting from the tail when `maxBufferSize` is reached.
    *
    * @param offset The event offset.
    *              Must be higher than the last appended entry's offset.
    * @param entry The buffer entry.
    */
  def push(offset: Offset, entry: TransactionLogUpdate): Unit =
    Timed.value(
      pushTimer,
      synchronized {
        _bufferLog.lastOption.foreach {
          // Encountering a non-strictly increasing offset is an error condition.
          case (lastOffset, _) if lastOffset >= offset =>
            throw UnorderedException(lastOffset, offset)
          case _ =>
        }

        if (maxBufferSize <= 0) {
          // Do nothing since buffer updates are not atomic and the reads are not synchronized.
          // This ensures that reads can never see data in the buffer.
        } else {
          ensureSize(maxBufferSize - 1)

          _bufferLog = _bufferLog :+ offset -> entry
          extractEntryFromMap(entry).foreach { case (key, value) =>
            _lookupMap = _lookupMap.updated(key, value)
          }
        }
      },
    )

  /** Returns a slice of events from the buffer.
    *
    * @param startExclusive The start exclusive bound of the requested range.
    * @param endInclusive The end inclusive bound of the requested range.
    * @param filter A lambda function that allows pre-filtering the buffered elements
    *               before assembling [[maxBufferedChunkSize]]-sized slices.
    * @return A slice of the series of events as an ordered vector satisfying the input bounds.
    */
  def slice[FILTER_RESULT](
      startExclusive: Offset,
      endInclusive: Offset,
      filter: TransactionLogUpdate => Option[FILTER_RESULT],
  ): BufferSlice[(Offset, FILTER_RESULT)] = {
    val vectorSnapshot = _bufferLog

    val bufferStartSearchResult = vectorSnapshot.view.map(_._1).search(startExclusive)
    val bufferEndSearchResult = vectorSnapshot.view.map(_._1).search(endInclusive)

    val bufferStartInclusiveIdx = indexAfter(bufferStartSearchResult)
    val bufferEndExclusiveIdx = indexAfter(bufferEndSearchResult)

    val bufferSlice = vectorSnapshot.slice(bufferStartInclusiveIdx, bufferEndExclusiveIdx)

    bufferStartSearchResult match {
      case InsertionPoint(0) if bufferSlice.isEmpty =>
        BufferSlice.LastBufferChunkSuffix(
          bufferedStartExclusive = endInclusive,
          slice = Vector.empty,
        )
      case InsertionPoint(0) => lastFilteredChunk(bufferSlice, filter, maxBufferedChunkSize)
      case InsertionPoint(_) | Found(_) =>
        BufferSlice.Inclusive(
          filterAndChunkSlice(bufferSlice.view, filter, maxBufferedChunkSize)
        )
    }
  }

  /** Lookup the accepted transaction update by transaction id. */
  def lookup(transactionId: TransactionId): Option[TransactionLogUpdate.TransactionAccepted] =
    _lookupMap.get(transactionId)

  /** Removes entries starting from the buffer head up until `endInclusive`.
    *
    * @param endInclusive The last inclusive (highest) buffer offset to be pruned.
    */
  def prune(endInclusive: Offset): Unit =
    Timed.value(
      pruneTimer,
      synchronized {
        val dropCount = _bufferLog.view.map(_._1).search(endInclusive) match {
          case Found(foundIndex) => foundIndex + 1
          case InsertionPoint(insertionPoint) => insertionPoint
        }

        dropOldest(dropCount)
      },
    )

  /** Remove all buffered entries */
  def flush(): Unit = synchronized {
    _bufferLog = Vector.empty
    _lookupMap = Map.empty
  }

  private def ensureSize(targetSize: Int): Unit = synchronized {
    val currentBufferLogSize = _bufferLog.size
    val currentLookupMapSize = _lookupMap.size

    if (currentLookupMapSize <= currentBufferLogSize) {
      bufferSizeHistogram.update(currentBufferLogSize)(MetricsContext.Empty)

      if (currentBufferLogSize > targetSize) {
        dropOldest(dropCount = currentBufferLogSize - targetSize)
      }
    } else {
      // This is an error condition. If encountered, clear the in-memory fan-out buffers.
      logger.withoutContext
        .error(
          s"In-memory fan-out lookup map size ($currentLookupMapSize) exceeds the buffer log size ($currentBufferLogSize). Clearing in-memory fan-out.."
        )

      flush()
    }
  }

  private def dropOldest(dropCount: Int): Unit = synchronized {
    val (evicted, remainingBufferLog) = _bufferLog.splitAt(dropCount)
    val lookupKeysToEvict =
      evicted.view.map(_._2).flatMap(extractEntryFromMap).map(_._2.transactionId)

    _bufferLog = remainingBufferLog
    _lookupMap = _lookupMap -- lookupKeysToEvict
  }

  private def extractEntryFromMap(
      transactionLogUpdate: TransactionLogUpdate
  ): Option[(TransactionId, TransactionLogUpdate.TransactionAccepted)] =
    transactionLogUpdate match {
      case txAccepted: TransactionLogUpdate.TransactionAccepted =>
        Some(txAccepted.transactionId -> txAccepted)
      case _: TransactionLogUpdate.TransactionRejected => None
    }
}

private[platform] object InMemoryFanoutBuffer {
  type TransactionId = String

  /** Specialized slice representation of a Vector */
  private[platform] sealed trait BufferSlice[+ELEM] extends Product with Serializable {
    def slice: Vector[ELEM]
  }

  object BufferSlice {

    /** A slice of a vector that is inclusive (start index of the slice in the source vector is gteq to 1) */
    private[platform] final case class Inclusive[ELEM](slice: Vector[ELEM])
        extends BufferSlice[ELEM]

    /** A slice of a vector that is a suffix of the requested window (i.e. start index of the slice in the source vector is 0) */
    private[platform] final case class LastBufferChunkSuffix[ELEM](
        bufferedStartExclusive: Offset,
        slice: Vector[ELEM],
    ) extends BufferSlice[ELEM]
  }

  private[cache] final case class UnorderedException[O](first: O, second: O)
      extends RuntimeException(
        s"Elements appended to the buffer should have strictly increasing offsets: $first vs $second"
      )

  private[cache] def indexAfter(bufferStartInclusiveSearchResult: SearchResult): Int =
    bufferStartInclusiveSearchResult match {
      case InsertionPoint(insertionPoint) => insertionPoint
      case Found(foundIndex) => foundIndex + 1
    }

  private[cache] def filterAndChunkSlice[FILTER_RESULT](
      sliceView: View[(Offset, TransactionLogUpdate)],
      filter: TransactionLogUpdate => Option[FILTER_RESULT],
      maxChunkSize: Int,
  ): Vector[(Offset, FILTER_RESULT)] =
    sliceView
      .flatMap { case (offset, entry) => filter(entry).map(offset -> _) }
      .take(maxChunkSize)
      .toVector

  private[cache] def lastFilteredChunk[FILTER_RESULT](
      bufferSlice: Vector[(Offset, TransactionLogUpdate)],
      filter: TransactionLogUpdate => Option[FILTER_RESULT],
      maxChunkSize: Int,
  ): BufferSlice.LastBufferChunkSuffix[(Offset, FILTER_RESULT)] = {
    val lastChunk =
      filterAndChunkSlice(bufferSlice.view.reverse, filter, maxChunkSize + 1).reverse

    if (lastChunk.isEmpty)
      BufferSlice.LastBufferChunkSuffix(bufferSlice.head._1, Vector.empty)
    else {
      // We waste the first element so we can pass it as the bufferStartExclusive
      BufferSlice.LastBufferChunkSuffix(lastChunk.head._1, lastChunk.tail)
    }
  }
}
