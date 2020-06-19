// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import scala.util.Random

class WeightedCacheSpec extends CacheSpecBase("a weighted cache") {
  implicit val `Int Weight`: Weight[Integer] = (_: Integer) => 1
  implicit val `String Weight`: Weight[String] = _.length.toLong

  override protected def newCache(): Cache[Integer, String] =
    WeightedCache.from[Integer, String](WeightedCache.Configuration(maximumWeight = 16))

  "a weighted cache" should {
    "evict values eventually, once the weight limit has been reached" in {
      val cache =
        WeightedCache.from[Integer, String](WeightedCache.Configuration(maximumWeight = 256))
      val values = Iterator.continually[Integer](Random.nextInt).take(1000).toSet.toVector

      values.foreach { value =>
        cache.get(value, _.toString)
      }
      val cachedValues = values.map(cache.getIfPresent).filter(_.isDefined)

      cachedValues.length should (be > 16 and be < 500)
    }
  }
}
