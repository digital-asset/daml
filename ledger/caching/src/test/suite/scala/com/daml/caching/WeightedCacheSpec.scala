// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import org.scalatest.wordspec.AnyWordSpec

class WeightedCacheSpec
    extends AnyWordSpec
    with ConcurrentCacheBehaviorSpecBase
    with ConcurrentCacheCachingSpecBase
    with ConcurrentCacheEvictionSpecBase {
  override protected lazy val name: String = "a weighted cache"

  override protected def newCache(): ConcurrentCache[Integer, String] =
    WeightedCache.from[Integer, String](WeightedCache.Configuration(maximumWeight = 16))

  override protected def newLargeCache(): ConcurrentCache[Integer, String] =
    WeightedCache.from[Integer, String](WeightedCache.Configuration(maximumWeight = 256))

  private implicit val `Int Weight`: Weight[Integer] = (_: Integer) => 1
  private implicit val `String Weight`: Weight[String] = _.length.toLong
}
