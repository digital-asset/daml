// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.{cache => caffeine}

object SizedCache {

  def from[Key <: AnyRef, Value <: AnyRef](configuration: Configuration): Cache[Key, Value] =
    from(configuration, None)

  def from[Key <: AnyRef, Value <: AnyRef](
      configuration: Configuration,
      metrics: CacheMetrics,
  ): Cache[Key, Value] =
    from(configuration, Some(metrics))

  private def from[Key <: AnyRef, Value <: AnyRef](
      configuration: Configuration,
      metrics: Option[CacheMetrics],
  ): Cache[Key, Value] =
    configuration match {
      case Configuration(maximumSize) if maximumSize <= 0 =>
        Cache.none
      case Configuration(maximumSize) =>
        val builder = caffeine.Caffeine
          .newBuilder()
          .maximumSize(maximumSize)
        CaffeineCache[Key, Value](builder, metrics)
    }

  final case class Configuration(maximumSize: Long) extends AnyVal

  object Configuration {

    val none: Configuration = Configuration(maximumSize = 0)

  }

}
