// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

import java.util.concurrent.ConcurrentMap

final class MapBackedCacheForTesting[Key, Value](store: ConcurrentMap[Key, Value])
    extends ConcurrentCache[Key, Value] {
  override def put(key: Key, value: Value): Unit = {
    store.put(key, value)
    ()
  }

  override def getIfPresent(key: Key): Option[Value] =
    Option(store.get(key))

  override def getOrAcquire(key: Key, acquire: Key => Value): Value =
    store.computeIfAbsent(key, acquire(_))
}
