// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class MappedCacheSpec
    extends AnyWordSpec
    with Matchers
    with ConcurrentCacheBehaviorSpecBase
    with ConcurrentCacheCachingSpecBase {
  override protected def name: String = "mapped cache"

  override protected def newCache(): ConcurrentCache[Integer, String] =
    new MapBackedCacheForTesting(new ConcurrentHashMap)

  name should {
    "transform the values into and out of the cache" in {
      val store: ConcurrentMap[Int, String] = new ConcurrentHashMap
      val cache: ConcurrentCache[Int, String] = new MapBackedCacheForTesting(store)
      val mappedCache = cache.mapValues[String](
        mapAfterReading = value => value.substring(6),
        mapBeforeWriting = value => Some("value " + value),
      )

      mappedCache.put(7, "seven")
      Option(store.get(7)) should be(Some("value seven"))
      mappedCache.getIfPresent(7) should be(Some("seven"))
    }

    "allow the mapping to change type" in {
      val store: ConcurrentMap[Int, String] = new ConcurrentHashMap
      val cache: ConcurrentCache[Int, String] = new MapBackedCacheForTesting(store)
      val mappedCache = cache.mapValues[Double](
        mapAfterReading = value => java.lang.Double.parseDouble(value),
        mapBeforeWriting = value => Some(value.toString),
      )

      mappedCache.put(7, 789.5)
      Option(store.get(7)) should be(Some("789.5"))
      mappedCache.getIfPresent(7) should be(Some(789.5))
    }

    "do not write if the mapping is lossy" in {
      val store: ConcurrentMap[Int, Int] = new ConcurrentHashMap
      val cache: ConcurrentCache[Int, Int] = new MapBackedCacheForTesting(store)
      val mappedCache = cache.mapValues[String](
        mapAfterReading = value => value.toString,
        mapBeforeWriting = value =>
          try {
            Some(Integer.parseInt(value))
          } catch {
            case _: NumberFormatException => None
          },
      )

      mappedCache.put(1, "one two three")
      mappedCache.put(7, "789")

      Option(store.get(1)) should be(None)
      Option(store.get(7)) should be(Some(789))

      mappedCache.getIfPresent(1) should be(None)
      mappedCache.getIfPresent(7) should be(Some("789"))
    }
  }
}
