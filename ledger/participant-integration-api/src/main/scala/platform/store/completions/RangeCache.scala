package com.daml.platform.store.completions

import com.daml.ledger.participant.state.v1.Offset

import scala.collection.SortedMap

case class Boundaries(startExclusive: Offset, endInclusive: Offset)

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
    * @param startExclusive
    * @param endInclusive
    * @param values
    * @return new instance of cache containing
    */
  def cache(
      startExclusive: Offset,
      endInclusive: Offset,
      values: SortedMap[Offset, T],
  ): RangeCache[T] = {
    // if cached offset and new offset are disjointed, cache newer one, else cache join
    if (boundaries.endInclusive < startExclusive) {
      copy(
        boundaries = Boundaries(startExclusive, endInclusive),
        cache = values.takeRight(maxElems),
      )
    } else if (boundaries.startExclusive > endInclusive) {
      this
    } else {
      copy(
        boundaries = Boundaries(
          startExclusive = getLesser(boundaries.startExclusive, startExclusive),
          endInclusive = getGreater(boundaries.endInclusive, endInclusive),
        ),
        cache = (values ++ cache).takeRight(maxElems),
      )
    }
  }

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
          cache = ((cache.range(startExclusive, endInclusive) - startExclusive) ++ cache)
            .get(endInclusive)
            .map(v => SortedMap(endInclusive -> v))
            .getOrElse(SortedMap.empty),
        )
      )

    }

  private def areDisjointed(startExclusive: Offset, endInclusive: Offset): Boolean =
    startExclusive >= boundaries.endInclusive || endInclusive <= boundaries.startExclusive

  private def getGreater(offset1: Offset, offset2: Offset): Offset =
    if (offset1 > offset2) offset1 else offset2

  private def getLesser(offset1: Offset, offset2: Offset): Offset =
    if (offset1 < offset2) offset1 else offset2
}

object RangeCache {

  /** creates empty range cache
    * @param maxElems
    * @tparam T
    * @return
    */
  def empty[T](maxElems: Int): RangeCache[T] = RangeCache(
    Boundaries(Offset.beforeBegin, Offset.beforeBegin),
    maxElems,
    SortedMap.empty[Offset, T],
  )
}
