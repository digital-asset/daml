// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

final class NoCache[Key, Value] private[caching] extends ConcurrentCache[Key, Value] {
  override def put(key: Key, value: Value): Unit = ()

  override def putAll(mappings: Map[Key, Value]): Unit = ()

  override def getIfPresent(key: Key): Option[Value] = None

  override def getOrAcquire(key: Key, acquire: Key => Value): Value = acquire(key)
}
