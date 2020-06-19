// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{Matchers, WordSpec}

class NoCacheSpec extends WordSpec with Matchers {
  "a non-existent cache" should {
    "compute the correct results" in {
      val cache = Cache.none[Int, String]

      cache.get(1, _.toString) should be("1")
      cache.get(2, _.toString) should be("2")
      cache.get(3, _.toString) should be("3")
      cache.get(2, _.toString) should be("2")
    }

    "compute every time" in {
      val cache = Cache.none[Int, String]
      val counter = new AtomicInteger(0)

      def compute(value: Int): String = {
        counter.incrementAndGet()
        value.toString
      }

      cache.get(1, compute)
      cache.get(1, compute)
      cache.get(1, compute)
      cache.get(2, compute)

      counter.get() should be(4)
    }

    "always return `None` on `getIfPresent`" in {
      val cache = Cache.none[Int, String]

      cache.getIfPresent(7) should be(None)
      cache.get(7, _.toString) should be("7")
      cache.getIfPresent(7) should be(None)
    }

    "do nothing on `put`" in {
      val cache = Cache.none[Int, String]

      cache.put(7, "7")
      cache.getIfPresent(7) should be(None)

      val counter = new AtomicInteger(0)

      def compute(value: Int): String = {
        counter.incrementAndGet()
        value.toString
      }

      cache.get(7, compute) should be("7")
      counter.get() should be(1)
    }
  }
}
