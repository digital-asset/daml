// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions

import com.daml.ledger.participant.state.v1.Offset

import scala.collection.SortedMap

case class Range(startExclusive: Offset, endInclusive: Offset) {
  import Range._

  def isBeforeBegin: Boolean =
    startExclusive == Offset.beforeBegin && endInclusive == Offset.beforeBegin

  /** Relative Complement of range in this (this - range) // TODO improve comments
    * @param range
    * @return
    */
  def lesserRangeDifference(range: Range): Option[Range] = {
    if (isSubsetOf(range)) {
      None
    } else {
      if (startExclusive >= range.startExclusive) {
        None
      } else {
        Some(Range(startExclusive, Range.getLesser(endInclusive, range.startExclusive)))
      }
    }
  }

  def greaterRangeDifference(range: Range): Option[Range] = {
    if (isSubsetOf(range)) {
      None
    } else {
      if (endInclusive <= range.endInclusive) {
        None
      } else {
        Some(Range(Range.getGreater(startExclusive, range.endInclusive), endInclusive))
      }
    }
  }

  def isSubsetOf(range: Range): Boolean =
    startExclusive >= range.startExclusive && endInclusive <= range.startExclusive

  def areDisjointed(range: Range): Boolean =
    startExclusive >= range.endInclusive || endInclusive <= range.startExclusive

  def intersect(range: Range): Option[Range] = {
    if (areDisjointed(range)) {
      None
    } else {
      Some(
        Range(
          getGreater(startExclusive, range.startExclusive),
          getLesser(endInclusive, range.endInclusive),
        )
      )
    }
  }
}

object Range {

  def getGreater(offset1: Offset, offset2: Offset): Offset =
    if (offset1 > offset2) offset1 else offset2

  def getLesser(offset1: Offset, offset2: Offset): Offset =
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
case class RangeCache[T](range: Range, maxItems: Int, cache: SortedMap[Offset, T]) {

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
      val start = calculateStartOffset(appendRange.startExclusive, values)
      copy(
        range = Range(start, appendRange.endInclusive),
        cache = values.takeRight(maxItems),
      )
    } else if (range.startExclusive > appendRange.endInclusive) {
      this
    } else {
      val allValues = values ++ cache
      val start =
        calculateStartOffset(
          Range.getLesser(range.startExclusive, appendRange.startExclusive),
          allValues,
        )
      copy(
        range = Range(
          startExclusive = start,
          endInclusive = Range.getGreater(range.endInclusive, appendRange.endInclusive),
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
        cache = {
          val cacheWithInvalidStart =
            cache.range(requestedRange.startExclusive, requestedRange.endInclusive) ++ cache
              .get(requestedRange.endInclusive)
              .map(v => SortedMap(requestedRange.endInclusive -> v))
              .getOrElse(SortedMap.empty[Offset, T])
          if (cacheWithInvalidStart.contains(requestedRange.startExclusive))
            cacheWithInvalidStart.tail
          else cacheWithInvalidStart
        },
      )
    }

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
