// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.offset.Offset
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.cache.BufferSlice.{
  BufferSlice,
  Empty,
  Inclusive,
  Prefix,
  WrappedEntry,
}
import com.daml.platform.store.cache.EventsBuffer.{
  BufferStateRef,
  RequestOffBufferBounds,
  SearchableByVector,
}

import scala.annotation.tailrec
import scala.collection.Searching.{Found, InsertionPoint, SearchResult}
import scala.concurrent.{ExecutionContext, Future}
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
//  private val sliceSize = bufferMetrics.sliceSize

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
  def slice[API_RESPONSE](
      startExclusive: Offset,
      endInclusive: Offset,
      filter: E => Future[Option[API_RESPONSE]],
      continue: Offset => () => Source[(Offset, API_RESPONSE), NotUsed],
  ): BufferSlice[(Offset, API_RESPONSE)] = if (lastPrunedOffset.exists(_ > startExclusive)) {
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
          Empty
        else {
          val Seq(bufferStartInclusiveIdx, bufferEndExclusiveIdx) =
            Seq(startExclusive, endInclusive)
              .map(bufferSnapshot.vector.searchBy(_, _._1))
              .map {
                case InsertionPoint(insertionPoint) => insertionPoint
                case Found(foundIndex) => foundIndex + 1
              }

          val vectorSlice =
            bufferSnapshot.vector.slice(bufferStartInclusiveIdx, bufferEndExclusiveIdx)

          if (vectorSlice.isEmpty) Empty
          else if (bufferStartInclusiveIdx == 0) {
            Prefix(
              vectorSlice.head._1,
              source(filter, vectorSlice.tail, continue),
            )
          } else Inclusive(source(filter, vectorSlice, continue))
        }
      },
    )

  private def source[API_RESPONSE](
      filter: E => Future[Option[API_RESPONSE]],
      inputSlice: Vector[(Offset, E)],
      continueFrom: Offset => () => Source[(Offset, API_RESPONSE), NotUsed],
  ): Source[(Offset, API_RESPONSE), NotUsed] =
    Source
      .lazySource(() => Source(inputSlice))
      .mapAsync(1) { case (offset, e) =>
        filter(e).map(_.map(offset -> _))(ExecutionContext.parasitic)
      }
      .mapConcat(_.iterator)
      .take(maxFetchSize.toLong)
      .buffer(maxFetchSize, OverflowStrategy.dropNew)
      .async
      .scan(BufferSlice.Zero: WrappedEntry[(Offset, API_RESPONSE)]) { case (scanned, response) =>
        val newIdx = scanned.idx + 1
        if (newIdx < maxFetchSize)
          BufferSlice.Scanned(newIdx, response)
        else if (newIdx == maxFetchSize) {
          BufferSlice.LastElement(maxFetchSize, response)
        } else throw new RuntimeException(s"Maximum $maxFetchSize elements expected")
      }
      .flatMapConcat {
        case BufferSlice.Zero => Source.empty
        case BufferSlice.Scanned(_, v) => Source.single(v)
        case BufferSlice.LastElement(_, v) =>
          Source.single(v).concatLazy(Source.lazySource(continueFrom(v._1)))
      }
      .mapMaterializedValue(_ => NotUsed)

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
  private[platform] final case object Empty extends BufferSlice[Nothing]

  /** A slice of a vector that is inclusive (start index of the slice in the source vector is gteq to 1) */
  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  private[platform] final case class Inclusive[ELEM](slice: Source[ELEM, NotUsed])
      extends BufferSlice[ELEM]

  /** A slice of a vector that is also the vector's prefix (i.e. start index of the slice in the source vector is 0) */
  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  private[platform] final case class Prefix[ELEM](
      headOffset: Offset,
      tail: Source[ELEM, NotUsed],
  ) extends BufferSlice[ELEM]
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
