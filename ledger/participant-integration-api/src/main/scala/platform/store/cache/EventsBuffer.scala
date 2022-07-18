// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.ledger.offset.Offset
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.cache.EventsBuffer.{
  BufferSlice,
  UnorderedException,
  filterAndChunkSlice,
  indexAfter,
  lastFilteredChunk,
}

import scala.collection.Searching.{Found, InsertionPoint, SearchResult}
import scala.collection.View

/** An ordered-by-offset queue buffer.
  *
  * The buffer allows appending only elements with strictly increasing offsets.
  *
  * @param maxBufferSize The maximum buffer size.
  * @param metrics The Daml metrics.
  * @param bufferQualifier The qualifier used for metrics tag specialization.
  * @tparam ENTRY The entry buffer type.
  */
class EventsBuffer[ENTRY](
    maxBufferSize: Int,
    metrics: Metrics,
    bufferQualifier: String,
    maxBufferedChunkSize: Int,
) {
  @volatile private[cache] var _bufferLog: Vector[(Offset, ENTRY)] = Vector.empty

  private val bufferMetrics = metrics.daml.services.index.Buffer(bufferQualifier)
  private val pushTimer = bufferMetrics.push
  private val sliceTimer = bufferMetrics.slice
  private val pruneTimer = bufferMetrics.prune
  private val sliceSizeHistogram = bufferMetrics.sliceSize
  private val bufferSizeHistogram = bufferMetrics.bufferSize

  /** Appends a new event to the buffer.
    *
    * Starts evicting from the tail when `maxBufferSize` is reached.
    *
    * @param offset The event offset.
    *              Must be higher than the last appended entry's offset.
    * @param entry The buffer entry.
    */
  def push(offset: Offset, entry: ENTRY): Unit =
    Timed.value(
      pushTimer,
      synchronized {
        _bufferLog.lastOption.foreach {
          // Ensures vector grows with strictly monotonic offsets.
          case (lastOffset, _) if lastOffset >= offset =>
            throw UnorderedException(lastOffset, offset)
          case _ =>
        }
        _bufferLog = (_bufferLog :+ offset -> entry).takeRight(maxBufferSize)
        bufferSizeHistogram.update(_bufferLog.size)
      },
    )

  /** Returns a slice of events from the buffer.
    *
    * @param startExclusive The start exclusive bound of the requested range.
    * @param endInclusive The end inclusive bound of the requested range.
    * @return A slice of the series of events as an ordered vector satisfying the input bounds.
    */
  def slice[FILTER_RESULT](
      startExclusive: Offset,
      endInclusive: Offset,
      filter: ENTRY => Option[FILTER_RESULT],
  ): BufferSlice[(Offset, FILTER_RESULT)] =
    Timed.value(
      sliceTimer, {
        val vectorSnapshot = _bufferLog

        val bufferStartSearchResult = vectorSnapshot.view.map(_._1).search(startExclusive)
        val bufferEndSearchResult = vectorSnapshot.view.map(_._1).search(endInclusive)

        val bufferStartInclusiveIdx = indexAfter(bufferStartSearchResult)
        val bufferEndExclusiveIdx = indexAfter(bufferEndSearchResult)

        val bufferSlice = vectorSnapshot.slice(bufferStartInclusiveIdx, bufferEndExclusiveIdx)

        val filteredBufferSlice = bufferStartSearchResult match {
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

        sliceSizeHistogram.update(filteredBufferSlice.slice.size)
        filteredBufferSlice
      },
    )

  /** Removes entries starting from the buffer tail up until `endInclusive`.
    *
    * @param endInclusive The last inclusive (highest) buffer offset to be pruned.
    */
  def prune(endInclusive: Offset): Unit =
    Timed.value(
      pruneTimer,
      synchronized {
        _bufferLog = _bufferLog.view.map(_._1).search(endInclusive) match {
          case Found(foundIndex) => _bufferLog.drop(foundIndex + 1)
          case InsertionPoint(insertionPoint) => _bufferLog.drop(insertionPoint)
        }
      },
    )

  /** Remove all buffered entries */
  def flush(): Unit = synchronized { _bufferLog = Vector.empty }
}

private[platform] object EventsBuffer {

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

  private[cache] def filterAndChunkSlice[ENTRY, FILTER_RESULT](
      sliceView: View[(Offset, ENTRY)],
      filter: ENTRY => Option[FILTER_RESULT],
      maxChunkSize: Int,
  ): Vector[(Offset, FILTER_RESULT)] =
    sliceView
      .flatMap { case (offset, entry) => filter(entry).map(offset -> _) }
      .take(maxChunkSize)
      .toVector

  private[cache] def lastFilteredChunk[ENTRY, FILTER_RESULT](
      bufferSlice: Vector[(Offset, ENTRY)],
      filter: ENTRY => Option[FILTER_RESULT],
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
