// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.wordspec.AnyWordSpec

class NoCacheSpec extends AnyWordSpec with ConcurrentCacheBehaviorSpecBase {
  override protected lazy val name: String = "a non-existent cache"

  override protected def newCache(): ConcurrentCache[Integer, String] =
    Cache.none

  "a non-existent cache" should {
    "compute every time" in {
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

      counter.get() should be(4)
    }

    "always return `None` on `getIfPresent`" in {
      val cache = Cache.none[Integer, String]

      cache.getIfPresent(7) should be(None)
      cache.getOrAcquire(7, _.toString) should be("7")
      cache.getIfPresent(7) should be(None)
    }

    "do nothing on `put`" in {
      val cache = Cache.none[Integer, String]

      cache.put(7, "7")
      cache.getIfPresent(7) should be(None)

      val counter = new AtomicInteger(0)

      def compute(value: Integer): String = {
        counter.incrementAndGet()
        value.toString
      }

      cache.getOrAcquire(7, compute) should be("7")
      counter.get() should be(1)
    }
  }
}
