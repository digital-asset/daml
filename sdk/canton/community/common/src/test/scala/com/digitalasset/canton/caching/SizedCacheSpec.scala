// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class SizedCacheSpec
    extends AnyWordSpec
    with ConcurrentCacheBehaviorSpecBase
    with ConcurrentCacheCachingSpecBase
    with ConcurrentCacheEvictionSpecBase {
  override protected lazy val name: String = "a sized cache"

  override protected def newCache()(implicit
      executionContext: ExecutionContext
  ): ConcurrentCache[Integer, String] =
    SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 16))

  override protected def newLargeCache()(implicit
      executionContext: ExecutionContext
  ): ConcurrentCache[Integer, String] =
    SizedCache.from[Integer, String](SizedCache.Configuration(maximumSize = 128))
}
