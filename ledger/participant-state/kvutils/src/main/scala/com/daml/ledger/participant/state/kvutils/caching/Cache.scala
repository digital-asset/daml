// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.caching

import com.github.benmanes.caffeine.{cache => caffeine}

import scala.collection.JavaConverters._

trait Cache[Key, Value] {
  def get(key: Key, acquire: Key => Value): Value

  def size: Size

  private[caching] def entries: Iterable[(Key, Value)]
}

object Cache {
  def none[Key, Value]: Cache[Key, Value] = new NoCache

  def maxWeight[Key <: AnyRef: Weight, Value <: AnyRef: Weight](weight: Size): Cache[Key, Value] =
    new CaffeineCache(
      caffeine.Caffeine
        .newBuilder()
        .maximumWeight(weight)
        .weigher[Key, Value](Weight.weigher)
        .build[Key, Value]())

  class NoCache[Key, Value] extends Cache[Key, Value] {
    override def get(key: Key, acquire: Key => Value): Value = acquire(key)

    override def size: Size = 0

    override private[caching] def entries: Iterable[(Key, Value)] = Iterable.empty
  }

  class CaffeineCache[Key, Value](val cache: caffeine.Cache[Key, Value]) extends Cache[Key, Value] {
    override def get(key: Key, acquire: Key => Value): Value =
      cache.get(key, key => acquire(key))

    override def size: Size =
      cache.estimatedSize()

    override private[caching] def entries: Iterable[(Key, Value)] =
      cache.asMap().asScala
  }
}
