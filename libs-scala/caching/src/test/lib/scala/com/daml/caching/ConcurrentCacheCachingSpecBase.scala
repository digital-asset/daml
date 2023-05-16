// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

trait ConcurrentCacheCachingSpecBase
    extends ConcurrentCacheSpecBase
    with AnyWordSpecLike
    with Matchers {
  name should {
    "compute once, and cache" in {
      val cache = newCache()
      val counter = new AtomicInteger(0)

      def compute(value: Integer): String = {
        counter.incrementAndGet()
        value.toString
      }

      cache.getOrAcquire(1, compute)
      cache.getOrAcquire(1, compute)
      cache.getOrAcquire(1, compute)
      cache.getOrAcquire(2, compute)

      counter.get() should be(2)
    }

    "return `None` on `getIfPresent` if the value is not present" in {
      val cache = newCache()

      cache.getIfPresent(7) should be(None)
    }

    "return the value on `getIfPresent` if the value is present" in {
      val cache = newCache()

      cache.getOrAcquire(7, _.toString) should be("7")
      cache.getIfPresent(7) should be(Some("7"))
    }

    "`put` values" in {
      val cache = newCache()

      cache.put(7, "7")
      cache.getIfPresent(7) should be(Some("7"))

      val counter = new AtomicInteger(0)

      def compute(value: Integer): String = {
        counter.incrementAndGet()
        value.toString
      }

      cache.getOrAcquire(7, compute) should be("7")
      counter.get() should be(0)
    }
  }

  "`putAll` values" in {
    val cache = newCache()

    cache.putAll(Map(Int.box(7) -> "7", Int.box(8) -> "8"))
    cache.getIfPresent(7) should be(Some("7"))
    cache.getIfPresent(8) should be(Some("8"))

    val counter = new AtomicInteger(0)

    def compute(value: Integer): String = {
      counter.incrementAndGet()
      value.toString
    }

    cache.getOrAcquire(7, compute) should be("7")
    cache.getOrAcquire(8, compute) should be("8")

    counter.get() should be(0)
  }

  "`invalidateAll` values" in {
    val cache = newCache()

    cache.putAll(Map(Int.box(7) -> "7", Int.box(8) -> "8"))
    cache.getIfPresent(7) should be(Some("7"))
    cache.getIfPresent(8) should be(Some("8"))

    cache.invalidateAll()

    cache.getIfPresent(7) should be(None)
    cache.getIfPresent(8) should be(None)
  }
}
