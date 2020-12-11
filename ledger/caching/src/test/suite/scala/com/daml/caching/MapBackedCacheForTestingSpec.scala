// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import java.util.concurrent.ConcurrentHashMap

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class MapBackedCacheForTestingSpec
    extends AnyWordSpec
    with Matchers
    with CacheBehaviorSpecBase
    with CacheCachingSpecBase {
  override protected def name: String = "map-backed cache"

  override protected def newCache(): ConcurrentCache[Integer, String] =
    new MapBackedCacheForTesting(new ConcurrentHashMap)
}
