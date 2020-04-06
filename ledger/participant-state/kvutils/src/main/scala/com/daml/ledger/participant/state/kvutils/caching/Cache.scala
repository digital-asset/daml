// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.caching

import com.google.common.{cache => google}

import scala.collection.JavaConverters._

trait Cache[Key, Value] {
  def get(key: Key, acquire: Key => Value): Value

  def size: Size

  private[caching] def entries: Iterable[(Key, Value)]
}

object Cache {
  def none[Key, Value]: Cache[Key, Value] = new None

  class None[Key, Value] extends Cache[Key, Value] {
    override def get(key: Key, acquire: Key => Value): Value = acquire(key)

    override def size: Size = 0

    override private[caching] def entries: Iterable[(Key, Value)] = Iterable.empty
  }

  implicit class `Google Cache to Cache`[Key, Value](val cache: google.Cache[Key, Value])
      extends Cache[Key, Value] {
    override def get(key: Key, acquire: Key => Value): Value =
      cache.get(key, () => acquire(key))

    override def size: Size =
      cache.size()

    override private[caching] def entries: Iterable[(Key, Value)] =
      cache.asMap().asScala
  }
}
