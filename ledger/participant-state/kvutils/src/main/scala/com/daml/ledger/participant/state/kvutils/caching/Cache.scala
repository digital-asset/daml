// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.caching

import com.github.benmanes.caffeine.{cache => caffeine}

import scala.compat.java8.OptionConverters._

trait Cache[Key, Value] {
  def get(key: Key, acquire: Key => Value): Value

  def size: Size

  def weight: Size
}

object Cache {
  def none[Key, Value]: Cache[Key, Value] = new NoCache

  def from[Key <: AnyRef: Weight, Value <: AnyRef: Weight](
      configuration: Configuration
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

  class NoCache[Key, Value] extends Cache[Key, Value] {
    override def get(key: Key, acquire: Key => Value): Value = acquire(key)

    override def size: Size = 0

    override def weight: Size = 0
  }

  class CaffeineCache[Key, Value](val cache: caffeine.Cache[Key, Value]) extends Cache[Key, Value] {
    override def get(key: Key, acquire: Key => Value): Value =
      cache.get(key, key => acquire(key))

    override def size: Size =
      cache.estimatedSize()

    override def weight: Size =
      cache.policy().eviction().asScala.flatMap(_.weightedSize().asScala).getOrElse(0)
  }
}
