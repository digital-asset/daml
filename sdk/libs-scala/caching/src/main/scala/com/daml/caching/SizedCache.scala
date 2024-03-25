// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.{cache => caffeine}

object SizedCache {

  def from[Key <: AnyRef, Value <: AnyRef](
      configuration: Configuration
  ): ConcurrentCache[Key, Value] =
    from(configuration, None)

  def from[Key <: AnyRef, Value <: AnyRef](
      configuration: Configuration,
      metrics: CacheMetrics,
  ): ConcurrentCache[Key, Value] =
    from(configuration, Some(metrics))

  private def from[Key <: AnyRef, Value <: AnyRef](
      configuration: Configuration,
      metrics: Option[CacheMetrics],
  ): ConcurrentCache[Key, Value] =
    configuration match {
      case Configuration(maximumSize) if maximumSize <= 0 =>
        Cache.none
      case Configuration(maximumSize) =>
        val builder = caffeine.Caffeine
          .newBuilder()
          .softValues()
          .maximumSize(maximumSize)
        CaffeineCache[Key, Value](builder, metrics)
    }

  final case class Configuration(maximumSize: Long) extends AnyVal

  object Configuration {

    val none: Configuration = Configuration(maximumSize = 0)

  }

}
