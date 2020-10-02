// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import org.scalatest.{Matchers, WordSpecLike}

trait CacheBehaviorSpecBase extends CacheSpecBase with WordSpecLike with Matchers {
  name should {
    "compute the correct results" in {
      val cache = newCache()

      cache.get(1, _.toString) should be("1")
      cache.get(2, _.toString) should be("2")
      cache.get(3, _.toString) should be("3")
      cache.get(2, _.toString) should be("2")
    }
  }
}
