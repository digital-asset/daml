// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import scala.util.Random

class SizedCacheSpec extends CacheSpecBase("a sized cache") {
  override protected def newCache(): Cache[Integer, String] =
    SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 16))

  "a sized cache" should {
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
