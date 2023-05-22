// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Second, Span}
import org.scalatest.wordspec.AnyWordSpecLike

import scala.util.Random

trait ConcurrentCacheEvictionSpecBase
    extends ConcurrentCacheBehaviorSpecBase
    with AnyWordSpecLike
    with Matchers
    with Eventually {
  override implicit def patienceConfig: PatienceConfig = PatienceConfig(scaled(Span(1, Second)))

  protected def newLargeCache(): ConcurrentCache[Integer, String]

  name should {
    "evict values eventually, once the limit has been reached" in {
      val cache = newLargeCache()
      val values = Iterator.continually[Integer](Random.nextInt()).take(1000).toSet.toVector

      values.foreach { value =>
        cache.getOrAcquire(value, _.toString)
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
