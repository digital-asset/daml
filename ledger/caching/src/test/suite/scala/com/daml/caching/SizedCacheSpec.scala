// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import org.scalatest.wordspec.AnyWordSpec

class SizedCacheSpec
    extends AnyWordSpec
    with CacheBehaviorSpecBase
    with CacheCachingSpecBase
    with CacheEvictionSpecBase {
  override protected lazy val name: String = "a sized cache"

  override protected def newCache(): Cache[Integer, String] =
    SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 16))

  override protected def newLargeCache(): Cache[Integer, String] =
    SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 128))
}
