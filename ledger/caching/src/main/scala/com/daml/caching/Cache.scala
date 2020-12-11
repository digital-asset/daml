// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

abstract class Cache[Key, Value] {
  def put(key: Key, value: Value): Unit

  def getIfPresent(key: Key): Option[Value]

  def mapValues[NewValue](
      from: Value => NewValue,
      to: NewValue => Option[Value],
  ): Cache[Key, NewValue] =
    new MappedCache(from, to)(this)
}

abstract class ConcurrentCache[Key, Value] extends Cache[Key, Value] {
  def getOrAcquire(key: Key, acquire: Key => Value): Value
}

object Cache {

  type Size = Long

  def none[Key, Value]: ConcurrentCache[Key, Value] = new NoCache
}
