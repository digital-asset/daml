// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{Matchers, WordSpecLike}

trait CacheCachingSpecBase extends CacheSpecBase with WordSpecLike with Matchers {
  name should {
    "compute once, and cache" in {
      val cache = newCache()
      val counter = new AtomicInteger(0)

      def compute(value: Integer): String = {
        counter.incrementAndGet()
        value.toString
      }

      cache.get(1, compute)
      cache.get(1, compute)
      cache.get(1, compute)
      cache.get(2, compute)

      counter.get() should be(2)
    }

    "return `None` on `getIfPresent` if the value is not present" in {
      val cache = newCache()

      cache.getIfPresent(7) should be(None)
    }

    "return the value on `getIfPresent` if the value is present" in {
      val cache = newCache()

      cache.get(7, _.toString) should be("7")
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

      cache.get(7, compute) should be("7")
      counter.get() should be(0)
    }
  }
}
