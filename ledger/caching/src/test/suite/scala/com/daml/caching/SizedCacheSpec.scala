// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Span}

import scala.util.Random

class SizedCacheSpec extends CacheSpecBase("a sized cache") with Eventually {
  override implicit def patienceConfig: PatienceConfig = PatienceConfig(scaled(Span(1, Second)))

  override protected def newCache(): Cache[Integer, String] =
    SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 16))

  "a sized cache" should {
    "evict values eventually, once the size limit has been reached" in {
      val cache =
        SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 128))
      val values = Iterator.continually[Integer](Random.nextInt).take(1000).toSet.toVector

      values.foreach { value =>
        cache.get(value, _.toString)
      }

      // The cache may not evict straight away. We should keep trying.
      eventually {
        val cachedValues = values.map(cache.getIfPresent).filter(_.isDefined)
        // It may evict more than expected, and it might grow past the bounds again before we check.
        cachedValues.length should (be > 16 and be < 500)
      }
    }
  }
}
