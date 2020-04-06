// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.validator.Cache._
import com.google.common.cache.{LoadingCache, Weigher}
import com.google.protobuf.MessageLite

import scala.collection.JavaConverters._

trait Cache[Key, +Value] {
  def get(key: Key): Value

  def size: Size

  protected def entries: Iterable[(Key, Value)]
}

object Cache {
  type Size = Long

  trait Weight[-T] {
    def weigh(value: T): Size
  }

  object Weight {
    def apply[T: Weight]: Weight[T] =
      implicitly[Weight[T]]

    def weigh[T: Weight](value: T): Size =
      Weight[T].weigh(value)

    def weigher[Key: Weight, Value: Weight]: Weigher[Key, Value] =
      new WeightWeigher[Key, Value]

    def ofCache[Key, Value](cache: Cache[Key, Value])(
        implicit keyWeight: Weight[Key],
        valueWeight: Weight[Value],
    ): Size =
      cache.entries.map { case (key, value) => keyWeight.weigh(key) + valueWeight.weigh(value) }.sum
  }

  implicit object `Bytes Weight` extends Weight[Bytes] {
    override def weigh(value: Bytes): Size =
      value.size().toLong
  }

  implicit object `Message Weight` extends Weight[MessageLite] {
    override def weigh(value: MessageLite): Size =
      value.getSerializedSize.toLong
  }

  class WeightWeigher[Key: Weight, Value: Weight] extends Weigher[Key, Value] {
    override def weigh(key: Key, value: Value): Int =
      (Weight.weigh(key) + Weight.weigh(value)).toInt
  }

  implicit class `LoadingCache to Cache`[Key, Value](val loadingCache: LoadingCache[Key, Value])
      extends Cache[Key, Value] {
    override def get(key: Key): Value =
      loadingCache.get(key)

    override def size: Size =
      loadingCache.size()

    protected override def entries: Iterable[(Key, Value)] =
      loadingCache.asMap().asScala
  }
}
