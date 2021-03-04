// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions

import com.daml.ledger.participant.state.v1.Offset

import scala.collection.SortedMap

case class Boundaries(startExclusive: Offset, endInclusive: Offset) {

  def isBeforeBegin: Boolean =
    startExclusive == Offset.beforeBegin && endInclusive == Offset.beforeBegin
}

/** In memory cache implementation for completions. If size of cache exceeds maxElems, oldest elements will be removed,
  * based on ledger offset
  *
  * @param boundaries offset boundaries of data stored in cache
  * @param maxElems maximum amount of elements stored in cache
  * @param cache map of cached elements
  * @tparam T type of cached elements
  */
case class RangeCache[T](boundaries: Boundaries, maxElems: Int, cache: SortedMap[Offset, T]) {

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
    if (boundaries.endInclusive < startExclusive) {
      val start = calculateStartOffset(startExclusive, values)
      copy(
        boundaries = Boundaries(start, endInclusive),
        cache = values.takeRight(maxElems),
      )
    } else if (boundaries.startExclusive > endInclusive) {
      this
    } else {
      val allValues = values ++ cache
      val start =
        calculateStartOffset(getLesser(boundaries.startExclusive, startExclusive), allValues)
      copy(
        boundaries = Boundaries(
          startExclusive = start,
          endInclusive = getGreater(boundaries.endInclusive, endInclusive),
        ),
        cache = allValues.takeRight(maxElems),
      )
    }
  }

  /**
   * fetches subset of current cache. If cached range and requested one are disjointed, then None is returned.
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
          boundaries = Boundaries(
            startExclusive = getGreater(startExclusive, boundaries.startExclusive),
            endInclusive = getLesser(endInclusive, boundaries.endInclusive),
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
    startExclusive >= boundaries.endInclusive || endInclusive <= boundaries.startExclusive

  private def getGreater(offset1: Offset, offset2: Offset): Offset =
    if (offset1 > offset2) offset1 else offset2

  private def getLesser(offset1: Offset, offset2: Offset): Offset =
    if (offset1 < offset2) offset1 else offset2

  private def calculateStartOffset(
      proposedStartOffset: Offset,
      proposedCacheUpdate: SortedMap[Offset, T],
  ) = {
    if (proposedCacheUpdate.size > maxElems)
      proposedCacheUpdate.toSeq(proposedCacheUpdate.size - maxElems - 1)._1
    else proposedStartOffset
  }
}

object RangeCache {

  /** creates empty range cache
    */
  def empty[T](maxElems: Int): RangeCache[T] = RangeCache(
    Boundaries(Offset.beforeBegin, Offset.beforeBegin),
    maxElems,
    SortedMap.empty[Offset, T],
  )
}
