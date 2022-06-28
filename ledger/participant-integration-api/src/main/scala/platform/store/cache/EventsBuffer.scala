// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.ledger.offset.Offset
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.cache.BufferSlice.{BufferSlice, Inclusive, LastBufferChunkSuffix}
import com.daml.platform.store.cache.EventsBuffer.{
  BufferState,
  RequestOffBufferBounds,
  SearchableByVector,
  UnorderedException,
  filterAndChunkSlice,
  indexAfter,
  lastFilteredChunk,
}

import scala.annotation.tailrec
import scala.collection.Searching.{Found, InsertionPoint, SearchResult}
import scala.collection.View
import scala.math.Ordering

/** An ordered-by-offset queue buffer.
  *
  * The buffer allows appending only elements with strictly increasing offsets.
  *
  * @param maxBufferSize The maximum buffer size.
  * @param metrics The Daml metrics.
  * @param bufferQualifier The qualifier used for metrics tag specialization.
  * @param isRangeEndMarker Identifies if an element [[ENTRY]] should be treated
  *                         as a range end marker, in which case the element would be treated
  *                         as a buffer range end updater and not appended to the actual buffer.
  * @tparam ENTRY The entry buffer type.
  */
final class EventsBuffer[ENTRY](
    maxBufferSize: Int,
    metrics: Metrics,
    bufferQualifier: String,
    isRangeEndMarker: ENTRY => Boolean,
    maxBufferedChunkSize: Int,
) {
  @volatile private var _bufferState = BufferState[Offset, ENTRY]()

  private val bufferMetrics = metrics.daml.services.index.Buffer(bufferQualifier)
  private val pushTimer = bufferMetrics.push
  private val sliceTimer = bufferMetrics.slice
  private val pruneTimer = bufferMetrics.prune
  private val sliceSizeHistogram = bufferMetrics.sliceSize

  /** Appends a new event to the buffer.
    *
    * Starts evicting from the tail when `maxBufferSize` is reached.
    *
    * @param offset The event offset.
    *              Must be higher than the last appended entry's offset, with the exception
    *              of the range end marker, which can have an offset equal to the last appended element.
    * @param entry The buffer entry.
    */
  def push(offset: Offset, entry: ENTRY): Unit =
    Timed.value(
      pushTimer,
      synchronized {
        _bufferState.rangeEnd.foreach { lastOffset =>
          // Ensure vector grows with strictly monotonic offsets.
          // Only specially-designated range end markers are allowed
          // to have offsets equal to the buffer range end.
          if (lastOffset > offset || (lastOffset == offset && !isRangeEndMarker(entry))) {
            throw UnorderedException(lastOffset, offset)
          }
        }

        var bufferVectorSnapshot = _bufferState.vector

        // The range end markers are not appended to the buffer
        if (!isRangeEndMarker(entry)) {
          if (bufferVectorSnapshot.size.toLong == maxBufferSize) {
            bufferVectorSnapshot = bufferVectorSnapshot.drop(1)
          }
          bufferVectorSnapshot = bufferVectorSnapshot :+ offset -> entry
        }

        // Update the buffer reference
        _bufferState = BufferState(bufferVectorSnapshot, Some(offset))
      },
    )

  /** Returns a slice of events from the buffer.
    *
    * Throws an exception if requested with `endInclusive` higher than the highest offset in the buffer.
    *
    * @param startExclusive The start exclusive bound of the requested range.
    * @param endInclusive The end inclusive bound of the requested range.
    * @return The series of events as an ordered vector satisfying the input bounds.
    */
  def slice[FILTER_RESULT](
      startExclusive: Offset,
      endInclusive: Offset,
      filter: ENTRY => Option[FILTER_RESULT],
  ): BufferSlice[(Offset, FILTER_RESULT)] =
    Timed.value(
      sliceTimer, {
        val bufferSnapshot = _bufferState
        val vectorSnapshot = bufferSnapshot.vector

        if (bufferSnapshot.rangeEnd.exists(_ < endInclusive)) {
          throw RequestOffBufferBounds(bufferSnapshot.rangeEnd.get, endInclusive)
        } else {
          val bufferStartSearchResult = vectorSnapshot.searchBy(startExclusive, _._1)
          val bufferEndSearchResult = vectorSnapshot.searchBy(endInclusive, _._1)

          val bufferStartInclusiveIdx = indexAfter(bufferStartSearchResult)
          val bufferEndExclusiveIdx = indexAfter(bufferEndSearchResult)

          val bufferSlice = vectorSnapshot.slice(bufferStartInclusiveIdx, bufferEndExclusiveIdx)

          val filteredBufferSlice = bufferStartSearchResult match {
            case InsertionPoint(0) if bufferSlice.isEmpty =>
              LastBufferChunkSuffix(endInclusive, Vector.empty)
            case InsertionPoint(0) => lastFilteredChunk(bufferSlice, filter, maxBufferedChunkSize)
            case InsertionPoint(_) | Found(_) =>
              Inclusive(filterAndChunkSlice(bufferSlice.view, filter, maxBufferedChunkSize))
          }

          sliceSizeHistogram.update(filteredBufferSlice.slice.size)
          filteredBufferSlice
        }
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
        _bufferState.vector.searchBy(endInclusive, _._1) match {
          case Found(foundIndex) =>
            _bufferState = _bufferState.copy(vector = _bufferState.vector.drop(foundIndex + 1))
          case InsertionPoint(insertionPoint) =>
            _bufferState = _bufferState.copy(vector = _bufferState.vector.drop(insertionPoint))
        }
      },
    )
}

private[platform] object BufferSlice {

  /** Specialized slice representation of a Vector */
  private[platform] sealed trait BufferSlice[+ELEM] extends Product with Serializable {
    def slice: Vector[ELEM]
  }

  /** A slice of a vector that is inclusive (start index of the slice in the source vector is gteq to 1) */
  private[platform] final case class Inclusive[ELEM](slice: Vector[ELEM]) extends BufferSlice[ELEM]

  /** A slice of a vector that is a suffix of the requested window (i.e. start index of the slice in the source vector is 0) */
  private[platform] final case class LastBufferChunkSuffix[ELEM](
      bufferedStartExclusive: Offset,
      slice: Vector[ELEM],
  ) extends BufferSlice[ELEM]
}

private[platform] object EventsBuffer {
  private final case class BufferState[O, E](
      vector: Vector[(O, E)] = Vector.empty,
      rangeEnd: Option[O] = Option.empty,
  )

  private[cache] final case class UnorderedException[O](first: O, second: O)
      extends RuntimeException(
        s"Elements appended to the buffer should have strictly increasing offsets: $first vs $second"
      )

  private[cache] final case class RequestOffBufferBounds[O](bufferEnd: O, requestEnd: O)
      extends RuntimeException(
        s"Request endInclusive ($requestEnd) is higher than bufferEnd ($bufferEnd)"
      )

  /** Binary search implementation inspired from scala.collection.Searching
    * which allows specifying the search predicate.
    *
    * @param v The vector where to search
    * @tparam E The element type
    */
  private[cache] implicit class SearchableByVector[E](v: Vector[E]) {
    // TODO: Remove this specialized implementation and use v.view.map(by).search(elem) from Scala 2.13+ when compatibility allows it.
    final def searchBy[O: Ordering](elem: O, by: E => O): SearchResult =
      binarySearch(elem, 0, v.length, by)

    @tailrec
    private def binarySearch[O](elem: O, from: Int, to: Int, by: E => O)(implicit
        ord: Ordering[O]
    ): SearchResult =
      if (to == from) InsertionPoint(from)
      else {
        val idx = from + (to - from - 1) / 2
        math.signum(ord.compare(elem, by(v(idx)))) match {
          case -1 => binarySearch(elem, from, idx, by)(ord)
          case 1 => binarySearch(elem, idx + 1, to, by)(ord)
          case _ => Found(idx)
        }
      }
  }

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
  ): LastBufferChunkSuffix[(Offset, FILTER_RESULT)] = {
    val lastChunk =
      filterAndChunkSlice(bufferSlice.view.reverse, filter, maxChunkSize + 1).reverse

    if (lastChunk.isEmpty)
      LastBufferChunkSuffix(bufferSlice.head._1, Vector.empty)
    else {
      // We waste the first element so we can pass it as the bufferStartExclusive
      LastBufferChunkSuffix(lastChunk.head._1, lastChunk.tail)
    }
  }
}
