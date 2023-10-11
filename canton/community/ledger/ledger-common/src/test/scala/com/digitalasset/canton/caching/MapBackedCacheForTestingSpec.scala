// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ConcurrentHashMap

final class MapBackedCacheForTestingSpec
    extends AnyWordSpec
    with Matchers
    with ConcurrentCacheBehaviorSpecBase
    with ConcurrentCacheCachingSpecBase {
  override def name: String = "map-backed cache"

  override protected def newCache(): ConcurrentCache[Integer, String] =
    new MapBackedCacheForTesting(new ConcurrentHashMap)
}
