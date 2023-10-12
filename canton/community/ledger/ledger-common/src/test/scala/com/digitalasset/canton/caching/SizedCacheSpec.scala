// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import org.scalatest.wordspec.AnyWordSpec

class SizedCacheSpec
    extends AnyWordSpec
    with ConcurrentCacheBehaviorSpecBase
    with ConcurrentCacheCachingSpecBase
    with ConcurrentCacheEvictionSpecBase {
  override protected lazy val name: String = "a sized cache"

  override protected def newCache(): ConcurrentCache[Integer, String] =
    SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 16))

  override protected def newLargeCache(): ConcurrentCache[Integer, String] =
    SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 128))
}
