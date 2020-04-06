// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.caching

import com.daml.ledger.participant.state.kvutils.Bytes
import com.google.common.{cache => google}
import com.google.protobuf.MessageLite

trait Weight[-T] {
  def weigh(value: T): Size
}

object Weight {
  def apply[T](implicit weight: Weight[T]): Weight[T] =
    weight

  def weigh[T](value: T)(implicit weight: Weight[T]): Size =
    weight.weigh(value)

  def weigher[Key: Weight, Value: Weight]: google.Weigher[Key, Value] =
    new WeightWeigher[Key, Value]

  def ofCache[Key, Value](cache: Cache[Key, Value])(
      implicit keyWeight: Weight[Key],
      valueWeight: Weight[Value],
  ): Size =
    cache.entries.map { case (key, value) => keyWeight.weigh(key) + valueWeight.weigh(value) }.sum

  implicit object `Bytes Weight` extends Weight[Bytes] {
    override def weigh(value: Bytes): Size =
      value.size().toLong
  }

  implicit object `Message Weight` extends Weight[MessageLite] {
    override def weigh(value: MessageLite): Size =
      value.getSerializedSize.toLong
  }

  class WeightWeigher[Key: Weight, Value: Weight] extends google.Weigher[Key, Value] {
    override def weigh(key: Key, value: Value): Int =
      (Weight.weigh(key) + Weight.weigh(value)).toInt
  }

}
