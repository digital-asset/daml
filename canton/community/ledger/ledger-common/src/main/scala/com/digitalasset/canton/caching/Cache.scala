// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

/** A cache. Used for caching values.
  *
  * The strategy used for eviction is implementation-dependent.
  *
  * @tparam Key   The type of the key used to look up values.
  * @tparam Value The type of the cached value.
  */
abstract class Cache[Key, Value] {

  /** Put a value into the cache.
    *
    * This may cause values to be evicted at some point in the future.
    */
  def put(key: Key, value: Value): Unit

  /** Put all the entries of `mappings` into the cache.
    */
  def putAll(mappings: Map[Key, Value]): Unit

  /** Retrieve a value by key, if it's present.
    */
  def getIfPresent(key: Key): Option[Value]

  /** Transform values when reading from or writing to the cache.
    *
    * Optionally allows the mapping to discard values by returning [[scala.None]] when transforming before
    * writing.
    *
    * @param mapAfterReading  Transform values after reading.
    * @param mapBeforeWriting Transform values before writing. Discards the value if the result is [[scala.None]].
    * @tparam NewValue The new value type.
    * @return A wrapped cache, backed by this cache.
    */
  def mapValues[NewValue](
      mapAfterReading: Value => NewValue,
      mapBeforeWriting: NewValue => Option[Value],
  ): Cache[Key, NewValue] =
    new MappedCache(mapAfterReading, mapBeforeWriting)(this)

  /** Removes all cached entries. */
  def invalidateAll(): Unit
}

/** A cache that is concurrency-safe. This means it is able to look up a value, and if it does not
  * exist, populate the value at that key, in a single, atomic action.
  *
  * @tparam Key   The type of the key used to look up values.
  * @tparam Value The type of the cached value.
  */
abstract class ConcurrentCache[Key, Value] extends Cache[Key, Value] {

  /** Retrieve a value by key if it's present. If there is no value, atomically computes the value
    * using the provided function, writes it to the cache, and returns it.
    */
  def getOrAcquire(key: Key, acquire: Key => Value): Value
}

object Cache {

  type Size = Long

  /** A cache that discards all writes, and therefore always responds with no value. */
  def none[Key, Value]: ConcurrentCache[Key, Value] = new NoCache
}
