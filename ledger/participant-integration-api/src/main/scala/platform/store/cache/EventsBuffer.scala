// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.cache.EventsBuffer.{
  BufferStateRef,
  RequestOffBufferBounds,
  SearchableByVector,
  UnorderedException,
}

import scala.annotation.tailrec
import scala.collection.Searching.{Found, InsertionPoint, SearchResult}
import scala.math.Ordering
import scala.math.Ordering.Implicits.infixOrderingOps

/** An ordered-by-offset ring buffer.
  *
  * The buffer allows appending only elements with strictly increasing offsets.
  *
  * @param maxBufferSize The maximum buffer size.
  * @param metrics The DAML metrics.
  * @param bufferQualifier The qualifier used for metrics tag specialization.
  * @param isRangeEndMarker Identifies if an element [[E]] should be treated
  *                         as a range end marker, in which case the element would be treated
  *                         as a buffer range end updater and not appended to the actual buffer.
  * @tparam O The offset type.
  * @tparam E The entry buffer type.
  */
private[cache] final class EventsBuffer[O: Ordering, E](
    maxBufferSize: Int,
    metrics: Metrics,
    bufferQualifier: String,
    isRangeEndMarker: E => Boolean,
) {
  @volatile private var _bufferStateRef = BufferStateRef[O, E]()

  private val pushTimer = metrics.daml.services.index.streamsBuffer.push(bufferQualifier)
  private val sliceTimer = metrics.daml.services.index.streamsBuffer.slice(bufferQualifier)
  private val pruneTimer = metrics.daml.services.index.streamsBuffer.prune(bufferQualifier)

  /** Appends a new event to the buffer.
    *
    * Starts evicting from the tail when `maxBufferSize` is reached.
    *
    * @param offset The event offset.
    *              Must be higher than the last appended entry's offset, with the exception
    *              of the range end marker, which can have an offset equal to the last appended element.
    * @param entry The buffer entry.
    */
  def push(offset: O, entry: E): Unit =
    Timed.value(
      pushTimer,
      synchronized {
        _bufferStateRef.rangeEnd.foreach { lastOffset =>
          // Ensure vector grows with strictly monotonic offsets.
          // Only specially-designated range end markers are allowed
          // to have offsets equal to the buffer range end.
          if (lastOffset > offset || (lastOffset == offset && !isRangeEndMarker(entry))) {
            throw UnorderedException(lastOffset, offset)
          }
        }

        var auxBufferVector = _bufferStateRef.vector

        // The range end markers are not appended to the buffer
        if (!isRangeEndMarker(entry)) {
          if (auxBufferVector.size == maxBufferSize) {
            auxBufferVector = auxBufferVector.drop(1)
          }
          auxBufferVector = auxBufferVector :+ offset -> entry
        }

        // Update the buffer reference
        _bufferStateRef = BufferStateRef(auxBufferVector, Some(offset))
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
  def slice(startExclusive: O, endInclusive: O): Vector[(O, E)] =
    Timed.value(
      sliceTimer, {
        val bufferSnapshot = _bufferStateRef
        if (bufferSnapshot.rangeEnd.exists(_ < endInclusive)) {
          throw RequestOffBufferBounds(bufferSnapshot.vector.last._1, endInclusive)
        } else if (bufferSnapshot.vector.isEmpty) {
          Vector.empty
        } else {
          val bufferEndExclusiveIdx = bufferSnapshot.vector.searchBy(endInclusive, _._1) match {
            case Found(foundIndex) => foundIndex + 1
            case InsertionPoint(insertionPoint) => insertionPoint
          }

          val bufferStartInclusiveIdx = bufferSnapshot.vector.searchBy(startExclusive, _._1) match {
            case InsertionPoint(insertionPoint) => insertionPoint
            case Found(foundIndex) => foundIndex + 1
          }

          bufferSnapshot.vector.slice(bufferStartInclusiveIdx, bufferEndExclusiveIdx)
        }
      },
    )

  /** Removes entries starting from the buffer tail up until `endInclusive`.
    *
    * @param endInclusive The last inclusive (highest) buffer offset to be pruned.
    */
  def prune(endInclusive: O): Unit =
    Timed.value(
      pruneTimer,
      synchronized {
        _bufferStateRef.vector.searchBy[O](endInclusive, _._1) match {
          case Found(foundIndex) =>
            _bufferStateRef =
              _bufferStateRef.copy(vector = _bufferStateRef.vector.drop(foundIndex + 1))
          case InsertionPoint(insertionPoint) =>
            _bufferStateRef =
              _bufferStateRef.copy(vector = _bufferStateRef.vector.drop(insertionPoint))
        }
      },
    )
}

private[cache] object EventsBuffer {
  private final case class BufferStateRef[O, E](
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
}
