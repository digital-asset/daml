// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions

import com.daml.ledger.participant.state.v1.Offset

import scala.collection.SortedMap

/** In memory cache implementation for completions. If size of cache exceeds maxItems, oldest elements will be removed,
  * based on ledger offset
  *
  * @param range offset range of data stored in cache
  * @param maxItems maximum amount of elements stored in cache
  * @param cache map of cached elements
  * @tparam T type of cached elements
  */
case class RangeCache[T] private (range: Range, maxItems: Int, cache: SortedMap[Offset, T]) {

  assert(
    !cache.lastOption.exists(_._1 > range.endInclusive) && !cache.headOption.exists(
      _._1 <= range.startExclusive
    ),
    "Cached values cannot be outside cache range",
  )

  assert(cache.size <= maxItems, "Cache size cannot exceeds maxItems param")

  /** Caches values. This method may remove oldest values (based on offset)
    * @param appendRange range of proposed cache
    * @param values proposed new values of cache
    * @return new instance of cache containing conjunction of existing cache and proposed cache limited to maxItems
    */
  def append(
      appendRange: Range,
      values: SortedMap[Offset, T],
  ): RangeCache[T] = {
    if (range.areConsecutive(appendRange) || !range.areDisjointed(appendRange)) {
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
    } else {
      if (range.startExclusive > appendRange.startExclusive) this
      else RangeCache(appendRange, maxItems, values)
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
    if (proposedCacheUpdate.size > maxItems) {
      val sortedSeq = proposedCacheUpdate.toSeq.sortBy(_._1)
      sortedSeq(proposedCacheUpdate.size - maxItems - 1)._1
    } else proposedStartOffset
  }
}
