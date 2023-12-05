// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

trait ConcurrentCacheBehaviorSpecBase
    extends ConcurrentCacheSpecBase
    with AnyWordSpecLike
    with Matchers {
  name should {
    "compute the correct results" in {
      val cache = newCache()

      cache.getOrAcquire(1, _.toString) should be("1")
      cache.getOrAcquire(2, _.toString) should be("2")
      cache.getOrAcquire(3, _.toString) should be("3")
      cache.getOrAcquire(2, _.toString) should be("2")
    }
  }
}
