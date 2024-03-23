// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import com.github.benmanes.caffeine.{cache => caffeine}

trait Weight[-T] {
  def weigh(value: T): Cache.Size
}

object Weight {
  def apply[T](implicit weight: Weight[T]): Weight[T] =
    weight

  def weigh[T](value: T)(implicit weight: Weight[T]): Cache.Size =
    weight.weigh(value)

  def weigher[Key: Weight, Value: Weight]: caffeine.Weigher[Key, Value] =
    new WeightWeigher[Key, Value]

  final class WeightWeigher[Key: Weight, Value: Weight] extends caffeine.Weigher[Key, Value] {
    override def weigh(key: Key, value: Value): Int =
      (Weight.weigh(key) + Weight.weigh(value)).toInt
  }
}
