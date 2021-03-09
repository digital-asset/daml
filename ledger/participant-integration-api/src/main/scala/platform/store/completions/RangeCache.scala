// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions

import com.daml.ledger.participant.state.v1.Offset

import scala.collection.SortedMap

case class Range(startExclusive: Offset, endInclusive: Offset) {

  def isBeforeBegin: Boolean =
    startExclusive == Offset.beforeBegin && endInclusive == Offset.beforeBegin
}

/** In memory cache implementation for completions. If size of cache exceeds maxItems, oldest elements will be removed,
  * based on ledger offset
  *
  * @param range offset range of data stored in cache
  * @param maxItems maximum amount of elements stored in cache
  * @param cache map of cached elements
  * @tparam T type of cached elements
  */
case class RangeCache[T](range: Range, maxItems: Int, cache: SortedMap[Offset, T]) {

  /** Caches values. This method may remove oldest values (based on offset)
    * @param startExclusive start of proposed cache
    * @param endInclusive end of proposed cache
    * @param values proposed new values of cache
    * @return new instance of cache containing conjunction of existing cache and proposed cache limited to maxItems
    */
  def cache(
      startExclusive: Offset,
      endInclusive: Offset,
      values: SortedMap[Offset, T],
  ): RangeCache[T] = {
    // if cached offset and new offset are disjointed, cache newer one, else cache join
    if (range.endInclusive < startExclusive) {
      val start = calculateStartOffset(startExclusive, values)
      copy(
        range = Range(start, endInclusive),
        cache = values.takeRight(maxItems),
      )
    } else if (range.startExclusive > endInclusive) {
      this
    } else {
      val allValues = values ++ cache
      val start =
        calculateStartOffset(getLesser(range.startExclusive, startExclusive), allValues)
      copy(
        range = Range(
          startExclusive = start,
          endInclusive = getGreater(range.endInclusive, endInclusive),
        ),
        cache = allValues.takeRight(maxItems),
      )
    }
  }

  /** fetches subset of current cache. If cached range and requested one are disjointed, then None is returned.
    * @param startExclusive requested subset start
    * @param endInclusive requested subset end
    * @return optional subset of current cache as new RangeCache instance.
    */
  def slice(startExclusive: Offset, endInclusive: Offset): Option[RangeCache[T]] =
    if (areDisjointed(startExclusive, endInclusive)) {
      None
    } else {
      Some(
        copy(
          range = Range(
            startExclusive = getGreater(startExclusive, range.startExclusive),
            endInclusive = getLesser(endInclusive, range.endInclusive),
          ),
          cache = {
            val cacheWithInvalidStart = cache.range(startExclusive, endInclusive) ++ cache
              .get(endInclusive)
              .map(v => SortedMap(endInclusive -> v))
              .getOrElse(SortedMap.empty[Offset, T])
            if (cacheWithInvalidStart.contains(startExclusive))
              cacheWithInvalidStart.tail
            else cacheWithInvalidStart
          },
        )
      )

    }

  private def areDisjointed(startExclusive: Offset, endInclusive: Offset): Boolean =
    startExclusive >= range.endInclusive || endInclusive <= range.startExclusive

  private def getGreater(offset1: Offset, offset2: Offset): Offset =
    if (offset1 > offset2) offset1 else offset2

  private def getLesser(offset1: Offset, offset2: Offset): Offset =
    if (offset1 < offset2) offset1 else offset2

  private def calculateStartOffset(
      proposedStartOffset: Offset,
      proposedCacheUpdate: SortedMap[Offset, T],
  ) = {
    if (proposedCacheUpdate.size > maxItems)
      proposedCacheUpdate.toSeq(proposedCacheUpdate.size - maxItems - 1)._1
    else proposedStartOffset
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
}
