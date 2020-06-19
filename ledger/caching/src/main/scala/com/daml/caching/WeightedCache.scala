// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.{cache => caffeine}

object WeightedCache {

  def from[Key <: AnyRef: Weight, Value <: AnyRef: Weight](
      configuration: Configuration,
  ): Cache[Key, Value] =
    from(configuration, None)

  def from[Key <: AnyRef: Weight, Value <: AnyRef: Weight](
      configuration: Configuration,
      metrics: CacheMetrics,
  ): Cache[Key, Value] =
    from(configuration, Some(metrics))

  private def from[Key <: AnyRef: Weight, Value <: AnyRef: Weight](
      configuration: Configuration,
      metrics: Option[CacheMetrics],
  ): Cache[Key, Value] =
    configuration match {
      case Configuration(maximumWeight) if maximumWeight <= 0 =>
        Cache.none
      case Configuration(maximumWeight) =>
        val builder =
          caffeine.Caffeine
            .newBuilder()
            .maximumWeight(maximumWeight)
            .weigher(Weight.weigher[Key, Value])
        metrics.fold(new CaffeineCache(builder))(new InstrumentedCaffeineCache(builder, _))
    }

}
