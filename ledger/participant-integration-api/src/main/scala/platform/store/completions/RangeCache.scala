// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions

import com.daml.ledger.participant.state.v1.Offset

import scala.collection.SortedMap

case class Range(startExclusive: Offset, endInclusive: Offset) {
  import Range._

  /** checks if range is set to before offset begin position
    */
  def isBeforeBegin: Boolean =
    startExclusive == Offset.beforeBegin && endInclusive == Offset.beforeBegin

  /** Relative complement of given rangeToDiff in this one (this - rangeToDiff) that is lesser than end of given rangeToDiff (this.end < rangeToDiff.start)
    * @return range representing set of offsets that are in this range but not in rangeToDiff where each offset is lesser than rangeToDiff.start
    */
  def lesserRangeDifference(rangeToDiff: Range): Option[Range] = {
    if (isSubsetOf(rangeToDiff)) {
      None
    } else {
      if (startExclusive >= rangeToDiff.startExclusive) {
        None
      } else {
        Some(Range(startExclusive, Range.min(endInclusive, rangeToDiff.startExclusive)))
      }
    }
  }

  /** Relative complement of given rangeToDiff in this one (this - rangeToDiff) that is greater than start of given rangeToDiff (this.start > rangeToDiff.end)
    * @return range representing set of offsets that are in this range but not in rangeToDiff where each offset is greater than rangeToDiff.end
    */
  def greaterRangeDifference(rangeToDiff: Range): Option[Range] = {
    if (isSubsetOf(rangeToDiff)) {
      None
    } else {
      if (endInclusive <= rangeToDiff.endInclusive) {
        None
      } else {
        Some(Range(Range.max(startExclusive, rangeToDiff.endInclusive), endInclusive))
      }
    }
  }

  /** checks if this range is subset of given one
    */
  def isSubsetOf(range: Range): Boolean =
    startExclusive >= range.startExclusive && endInclusive <= range.startExclusive

  /** checks if this and given range are disjointed - they have no offset in common.
    */
  def areDisjointed(range: Range): Boolean =
    startExclusive >= range.endInclusive || endInclusive <= range.startExclusive

  /** returns intersection of this and given range - range representing set of offsets that are in this and given range.
    */
  def intersect(range: Range): Option[Range] = {
    if (areDisjointed(range)) {
      None
    } else {
      Some(
        Range(
          max(startExclusive, range.startExclusive),
          min(endInclusive, range.endInclusive),
        )
      )
    }
  }
}

object Range {

  def max(offset1: Offset, offset2: Offset): Offset =
    if (offset1 > offset2) offset1 else offset2

  def min(offset1: Offset, offset2: Offset): Offset =
    if (offset1 < offset2) offset1 else offset2
}

/** In memory cache implementation for completions. If size of cache exceeds maxItems, oldest elements will be removed,
  * based on ledger offset
  *
  * @param range offset range of data stored in cache
  * @param maxItems maximum amount of elements stored in cache
  * @param cache map of cached elements
  * @tparam T type of cached elements
  */
case class RangeCache[T] private (range: Range, maxItems: Int, cache: SortedMap[Offset, T]) {

  /** Caches values. This method may remove oldest values (based on offset)
    * @param appendRange range of proposed cache
    * @param values proposed new values of cache
    * @return new instance of cache containing conjunction of existing cache and proposed cache limited to maxItems
    */
  def append(
      appendRange: Range,
      values: SortedMap[Offset, T],
  ): RangeCache[T] = { // add to cache greaterdiff
    // if cached offset and new offset are disjointed, cache newer one, else cache join
    if (range.endInclusive < appendRange.startExclusive) {
      val start = RangeCache.calculateStartOffset(maxItems, appendRange.startExclusive, values)
      copy(
        range = Range(start, appendRange.endInclusive),
        cache = values.takeRight(maxItems),
      )
    } else if (range.startExclusive > appendRange.endInclusive) {
      this
    } else {
      val allValues = values ++ cache
      val start =
        RangeCache.calculateStartOffset(
          maxItems,
          Range.min(range.startExclusive, appendRange.startExclusive),
          allValues,
        )
      copy(
        range = Range(
          startExclusive = start,
          endInclusive = Range.max(range.endInclusive, appendRange.endInclusive),
        ),
        cache = allValues.takeRight(maxItems),
      )
    }
  }

  /** fetches subset of current cache. If cached range and requested one are disjointed, then None is returned.
    * @param requestedRange requested subset range
    * @return optional subset of current cache as new RangeCache instance.
    */
  def slice(requestedRange: Range): Option[RangeCache[T]] =
    range.intersect(requestedRange).map { sliceRange =>
      copy(
        range = sliceRange,
        cache = rangeCache(requestedRange),
      )
    }

  /** performs range operation on cache SortedMap and applies (startInclusive, endExclusive) boundaries,
    * instead of used by scala.collection.SortedMap.range (startInclusive, endExclusive) boundaries
    */
  private def rangeCache(requestedRange: Range): SortedMap[Offset, T] = {
    val cacheWithInvalidStart =
      cache.range(requestedRange.startExclusive, requestedRange.endInclusive) ++ cache
        .get(requestedRange.endInclusive)
        .map(v => SortedMap(requestedRange.endInclusive -> v))
        .getOrElse(SortedMap.empty[Offset, T])
    if (cacheWithInvalidStart.contains(requestedRange.startExclusive))
      cacheWithInvalidStart.tail
    else cacheWithInvalidStart
  }
}

object RangeCache {

  /** creates empty range cache
    */
  def empty[T](maxItems: Int): RangeCache[T] = RangeCache(
    Range(Offset.beforeBegin, Offset.beforeBegin),
    maxItems,
    SortedMap.empty[Offset, T],
  )

  def apply[T](range: Range, maxItems: Int, cache: SortedMap[Offset, T]): RangeCache[T] =
    new RangeCache(
      range.copy(startExclusive = calculateStartOffset(maxItems, range.startExclusive, cache)),
      maxItems,
      cache.takeRight(maxItems),
    )

  private def calculateStartOffset[T](
      maxItems: Int,
      proposedStartOffset: Offset,
      proposedCacheUpdate: SortedMap[Offset, T],
  ) = {
    if (proposedCacheUpdate.size > maxItems)
      proposedCacheUpdate.toSeq(proposedCacheUpdate.size - maxItems - 1)._1
    else proposedStartOffset
  }
}
