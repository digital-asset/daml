// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import org.scalatest.WordSpec

class WeightedCacheSpec
    extends WordSpec
    with CacheBehaviorSpecBase
    with CacheCachingSpecBase
    with CacheEvictionSpecBase {
  override protected lazy val name: String = "a weighted cache"

  override protected def newCache(): Cache[Integer, String] =
    WeightedCache.from[Integer, String](WeightedCache.Configuration(maximumWeight = 16))

  override protected def newLargeCache(): Cache[Integer, String] =
    WeightedCache.from[Integer, String](WeightedCache.Configuration(maximumWeight = 256))

  private implicit val `Int Weight`: Weight[Integer] = (_: Integer) => 1
  private implicit val `String Weight`: Weight[String] = _.length.toLong
}
