// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

class SizedCacheSpec extends WordSpec with Matchers {
  "a sized cache" should {
    "compute the correct results" in {
      val cache =
        SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 16))

      cache.get(1, _.toString) should be("1")
      cache.get(2, _.toString) should be("2")
      cache.get(3, _.toString) should be("3")
      cache.get(2, _.toString) should be("2")
    }

    "compute once, and cache" in {
      val cache =
        SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 16))
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
      val cache =
        SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 16))

      cache.getIfPresent(7) should be(None)
    }

    "return the value on `getIfPresent` if the value is present" in {
      val cache =
        SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 16))

      cache.get(7, _.toString) should be("7")
      cache.getIfPresent(7) should be(Some("7"))
    }

    "`put` values" in {
      val cache =
        SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 16))

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

    "evict values eventually, once the size limit has been reached" in {
      val cache =
        SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 256))
      val values = Iterator.continually[Integer](Random.nextInt).take(1000).toSet.toVector

      values.zipWithIndex.foreach {
        case (value, index) =>
          (0 to index).foreach { _ =>
            cache.get(value, _.toString)
          }
      }
      val cachedValues = values.map(cache.getIfPresent).filter(_.isDefined)

      cachedValues.length should (be > 16 and be < 500)
    }
  }
}
