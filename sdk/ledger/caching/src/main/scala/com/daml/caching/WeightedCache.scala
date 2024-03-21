// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.{cache => caffeine}

object WeightedCache {

  def from[Key <: AnyRef: Weight, Value <: AnyRef: Weight](
      configuration: Configuration
  ): ConcurrentCache[Key, Value] =
    from(configuration, None)

  def from[Key <: AnyRef: Weight, Value <: AnyRef: Weight](
      configuration: Configuration,
      metrics: CacheMetrics,
  ): ConcurrentCache[Key, Value] =
    from(configuration, Some(metrics))

  private def from[Key <: AnyRef: Weight, Value <: AnyRef: Weight](
      configuration: Configuration,
      metrics: Option[CacheMetrics],
  ): ConcurrentCache[Key, Value] =
    configuration match {
      case Configuration(maximumWeight) if maximumWeight <= 0 =>
        Cache.none
      case Configuration(maximumWeight) =>
        val builder =
          caffeine.Caffeine
            .newBuilder()
            .softValues()
            .maximumWeight(maximumWeight)
            .weigher(Weight.weigher[Key, Value])
        CaffeineCache(builder, metrics)
    }

  final case class Configuration(maximumWeight: Long) extends AnyVal

  object Configuration {

    val none: Configuration = Configuration(maximumWeight = 0)

  }

}
