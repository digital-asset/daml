// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.offset.Offset
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.cache.BufferSlice.{
  BufferSlice,
  EmptyBuffer,
  EmptyPrefix,
  EmptyResult,
  Inclusive,
  Prefix,
}
import com.daml.platform.store.cache.EventsBuffer.{
  BufferStateRef,
  RequestOffBufferBounds,
  SearchableByVector,
}

import scala.annotation.tailrec
import scala.collection.Searching.{Found, InsertionPoint, SearchResult}
import scala.math.Ordering

/** An ordered-by-offset ring buffer.
  *
  * The buffer allows appending only elements with strictly increasing offsets.
  *
  * @param maxBufferSize The maximum buffer size.
  * @param metrics The Daml metrics.
  * @param bufferQualifier The qualifier used for metrics tag specialization.
  * @param ignoreMarker Identifies if an element [[E]] should be treated
  *                         as a range end marker, in which case the element would be treated
  *                         as a buffer range end updater and not appended to the actual buffer.
  * @tparam O The offset type.
  * @tparam E The entry buffer type.
  */
final class EventsBuffer[E](
    maxBufferSize: Long,
    metrics: Metrics,
    bufferQualifier: String,
    ignoreMarker: E => Boolean,
    // TODO LLP Specifically low to observe the cap in benchmarks. In prod setups we should use higher values
    maxFetchSize: Int = 128,
) {
  @volatile private var _bufferStateRef = BufferStateRef[Offset, E]()
  private var lastPrunedOffset = Option.empty[Offset]

  private val bufferMetrics = metrics.daml.services.index.Buffer(bufferQualifier)
  private val pushTimer = bufferMetrics.push
  private val sliceTimer = bufferMetrics.slice
  private val pruneTimer = bufferMetrics.prune
  private val sliceSizeHistogram = bufferMetrics.sliceSize

  def flush(): Unit = _bufferStateRef = BufferStateRef[Offset, E]()

  /** Appends a new event to the buffer.
    *
    * Starts evicting from the tail when `maxBufferSize` is reached.
    *
    * @param offset The event offset.
    *              Must be higher than the last appended entry's offset, with the exception
    *              of the range end marker, which can have an offset equal to the last appended element.
    * @param entry The buffer entry.
    */
  def push(offset: Offset, entry: E): Unit =
    Timed.value(
      pushTimer,
      synchronized {
        // Only updates with strictly increasing offsets are included in the buffer
        // Ensures idempotency in case of replayed updates.
        if (offset > _bufferStateRef.rangeEnd.getOrElse(Offset.beforeBegin)) {
          var auxBufferVector = _bufferStateRef.vector

          // The range end markers are not appended to the buffer
          if (!ignoreMarker(entry)) {
            if (auxBufferVector.size.toLong == maxBufferSize) {
              auxBufferVector = auxBufferVector.drop(1)
            }
            auxBufferVector = auxBufferVector :+ offset -> entry
          }

          // Update the buffer reference
          _bufferStateRef = BufferStateRef(auxBufferVector, Some(offset))
        }
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
      filter: E => Option[FILTER_RESULT],
  ): BufferSlice[(Offset, FILTER_RESULT)] = if (lastPrunedOffset.exists(_ > startExclusive)) {
    throw LedgerApiErrors.RequestValidation.ParticipantPrunedDataAccessed
      .Reject(
        cause =
          s"Buffered entries request from ${startExclusive.toHexString} to ${endInclusive.toHexString} precedes pruned offset ${lastPrunedOffset.get.toHexString}",
        earliestOffset = lastPrunedOffset.get.toHexString,
      )(DamlContextualizedErrorLogger.forTesting(getClass))
      .asGrpcError
  } else
    Timed.value(
      sliceTimer, {
        val bufferSnapshot = _bufferStateRef

        if (bufferSnapshot.rangeEnd.exists(_ < endInclusive)) {
          throw RequestOffBufferBounds(bufferSnapshot.vector.last._1, endInclusive)
        } else if (bufferSnapshot.vector.isEmpty)
          EmptyBuffer
        else {
          val Seq(bufferStartInclusiveIdx, bufferEndExclusiveIdx) =
            Seq(startExclusive, endInclusive)
              .map(bufferSnapshot.vector.searchBy(_, _._1))
              .map {
                case InsertionPoint(insertionPoint) => insertionPoint
                case Found(foundIndex) => foundIndex + 1
              }

          var lastOffset = Offset.beforeBegin
          val vSlice = bufferSnapshot.vector
            .slice(bufferStartInclusiveIdx, bufferEndExclusiveIdx)
          val filteredSliced = vSlice.iterator
            .map { case (offset, in) =>
              lastOffset = offset
              filter(in).map(offset -> _)
            }
            .collect { case Some(v) => v }
            .take(maxFetchSize)
            .toVector

          val sliceSize = filteredSliced.size
          sliceSizeHistogram.update(sliceSize)

          if (filteredSliced.isEmpty) {
            if (bufferStartInclusiveIdx == 0) {
              if (vSlice.isEmpty) EmptyBuffer
              else EmptyPrefix(vSlice.head._1)
            } else EmptyResult
          } else {
            val continue = Option.when(sliceSize == maxFetchSize)(lastOffset)

            if (bufferStartInclusiveIdx == 0)
              Prefix(
                filteredSliced.head._1,
                filteredSliced.tail,
                continue,
              )
            else Inclusive(filteredSliced, continue)
          }
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
        lastPrunedOffset = Some(endInclusive)
        _bufferStateRef.vector.searchBy(endInclusive, _._1) match {
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

private[platform] object BufferSlice {
  sealed trait WrappedEntry[+T] {
    def idx: Int
  }
  final case object Zero extends WrappedEntry[Nothing] {
    val idx: Int = 0
  }
  final case class Scanned[T](idx: Int, response: T) extends WrappedEntry[T]

  final case class LastElement[T](idx: Int, response: T) extends WrappedEntry[T]

  /** Specialized slice representation of a Vector */
  private[platform] sealed trait BufferSlice[+ELEM] extends Product with Serializable

  /** The source was empty */
  private[platform] final case object EmptyBuffer extends BufferSlice[Nothing]

  /** A slice of a vector that is inclusive (start index of the slice in the source vector is gteq to 1) */
  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  private[platform] final case class Inclusive[ELEM](
      slice: Vector[ELEM],
      continueFrom: Option[Offset],
  ) extends BufferSlice[ELEM]

  /** A slice of a vector that is also the vector's prefix (i.e. start index of the slice in the source vector is 0) */
  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  private[platform] final case class Prefix[ELEM](
      headOffset: Offset,
      tail: Vector[ELEM],
      continueFrom: Option[Offset],
  ) extends BufferSlice[ELEM]

  /** A slice of a vector that is also the vector's prefix (i.e. start index of the slice in the source vector is 0) */
  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  private[platform] final case object EmptyResult extends BufferSlice[Nothing]

  /** A slice of a vector that is also the vector's prefix (i.e. start index of the slice in the source vector is 0) */
  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  private[platform] final case class EmptyPrefix[ELEM](headOffset: Offset) extends BufferSlice[ELEM]
}

private[platform] object EventsBuffer {
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
