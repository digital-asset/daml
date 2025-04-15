// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import com.github.benmanes.caffeine.cache as caffeine

import scala.concurrent.ExecutionContext

object SizedCache {

  def from[Key <: AnyRef, Value <: AnyRef](
      configuration: Configuration
  )(implicit executionContext: ExecutionContext): ConcurrentCache[Key, Value] =
    from(configuration, None)

  def from[Key <: AnyRef, Value <: AnyRef](
      configuration: Configuration,
      metrics: CacheMetrics,
  )(implicit executionContext: ExecutionContext): ConcurrentCache[Key, Value] =
    from(configuration, Some(metrics))

  private def from[Key <: AnyRef, Value <: AnyRef](
      configuration: Configuration,
      metrics: Option[CacheMetrics],
  )(implicit executionContext: ExecutionContext): ConcurrentCache[Key, Value] =
    configuration match {
      case Configuration(maximumSize) if maximumSize <= 0 =>
        Cache.none
      case Configuration(maximumSize) =>
        val builder = caffeine.Caffeine
          .newBuilder()
          .softValues()
          .maximumSize(maximumSize)
          .executor(executionContext.execute(_))
        CaffeineCache[Key, Value](builder, metrics)
    }

  final case class Configuration(maximumSize: Long) extends AnyVal

  object Configuration {

    val none: Configuration = Configuration(maximumSize = 0)

  }

}
