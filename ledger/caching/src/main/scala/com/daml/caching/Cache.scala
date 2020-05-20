// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.codahale.metrics.Gauge
import com.daml.metrics.CacheMetrics
import com.github.benmanes.caffeine.{cache => caffeine}

import scala.compat.java8.OptionConverters._

sealed abstract class Cache[Key, Value] {
  def put(key: Key, value: Value): Unit

  def get(key: Key, acquire: Key => Value): Value

  def getIfPresent(key: Key): Option[Value]
}

object Cache {

  type Size = Long

  def none[Key, Value]: Cache[Key, Value] = new NoCache

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
        none
      case Configuration(maximumWeight) =>
        val builder =
          caffeine.Caffeine
            .newBuilder()
            .maximumWeight(maximumWeight)
            .weigher(Weight.weigher[Key, Value])
        metrics.fold(new CaffeineCache(builder))(new InstrumentedCaffeineCache(builder, _))
    }

  final class NoCache[Key, Value] private[Cache] extends Cache[Key, Value] {
    override def put(key: Key, value: Value): Unit = ()

    override def get(key: Key, acquire: Key => Value): Value = acquire(key)

    override def getIfPresent(key: Key): Option[Value] = None
  }

  sealed class CaffeineCache[Key, Value] private[Cache] (builder: caffeine.Caffeine[Key, Value])
      extends Cache[Key, Value] {

    private val cache = init(builder)

    protected def init(builder: caffeine.Caffeine[Key, Value]): caffeine.Cache[Key, Value] =
      builder.build()

    override def put(key: Key, value: Value): Unit = cache.put(key, value)

    override def get(key: Key, acquire: Key => Value): Value =
      cache.get(key, key => acquire(key))

    override def getIfPresent(key: Key): Option[Value] =
      Option(cache.getIfPresent(key))

  }

  private def size(cache: caffeine.Cache[_, _]): Gauge[Long] =
    () => cache.estimatedSize()

  private def weight(cache: caffeine.Cache[_, _]): Gauge[Long] =
    () => cache.policy().eviction().asScala.flatMap(_.weightedSize.asScala).getOrElse(0)

  final class InstrumentedCaffeineCache[Key, Value] private[Cache] (
      builder: caffeine.Caffeine[Key, Value],
      metrics: CacheMetrics,
  ) extends CaffeineCache[Key, Value](builder) {

    override protected def init(
        builder: caffeine.Caffeine[Key, Value],
    ): caffeine.Cache[Key, Value] = {
      val cache = super.init(builder.recordStats(() => new DropwizardStatsCounter(metrics)))
      metrics.registerSizeGauge(size(cache))
      metrics.registerWeightGauge(weight(cache))
      cache
    }

  }

}
