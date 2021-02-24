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
    * @tparam T
    * @return new instance of cache containing
    */
  def cache[T](
      startExclusive: Offset,
      endInclusive: Offset,
      values: SortedMap[Offset, T],
  ): RangeCache[T] = ???
}

object RangeCache {

  /**
   * creates empty range cache
   * @param maxElems
   * @tparam T
   * @return
   */
  def empty[T](maxElems: Int): RangeCache[T] = ???
}
