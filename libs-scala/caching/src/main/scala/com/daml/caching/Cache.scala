// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.github.benmanes.caffeine.{cache => caffeine}

import scala.compat.java8.OptionConverters._

sealed abstract class Cache[Key, Value] {
  def get(key: Key, acquire: Key => Value): Value

  def size: Cache.Size

  def weight: Cache.Size
}

object Cache {

  type Size = Long

  def none[Key, Value]: Cache[Key, Value] = new NoCache

  def from[Key <: AnyRef: Weight, Value <: AnyRef: Weight](
      configuration: Configuration,
  ): Cache[Key, Value] =
    configuration match {
      case Configuration(0) =>
        none
      case Configuration(maximumWeight) =>
        new CaffeineCache(
          caffeine.Caffeine
            .newBuilder()
            .maximumWeight(maximumWeight)
            .weigher(Weight.weigher[Key, Value])
            .build[Key, Value]())
    }

  final class NoCache[Key, Value] private[Cache] extends Cache[Key, Value] {
    override def get(key: Key, acquire: Key => Value): Value = acquire(key)

    override val size: Cache.Size = 0

    override val weight: Cache.Size = 0
  }

  final class CaffeineCache[Key, Value] private[Cache] (val cache: caffeine.Cache[Key, Value])
      extends Cache[Key, Value] {
    override def get(key: Key, acquire: Key => Value): Value =
      cache.get(key, key => acquire(key))

    override def size: Cache.Size =
      cache.estimatedSize()

    override def weight: Cache.Size =
      cache.policy().eviction().asScala.flatMap(_.weightedSize().asScala).getOrElse(0)
  }

}
