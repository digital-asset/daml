// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import java.util.concurrent.ConcurrentMap
import scala.jdk.CollectionConverters.*

final class MapBackedCacheForTesting[Key, Value](store: ConcurrentMap[Key, Value])
    extends ConcurrentCache[Key, Value] {
  override def put(key: Key, value: Value): Unit = {
    store.put(key, value)
    ()
  }

  override def putAll(mappings: Map[Key, Value]): Unit =
    store.putAll(mappings.asJava)

  override def getIfPresent(key: Key): Option[Value] =
    Option(store.get(key))

  override def getOrAcquire(key: Key, acquire: Key => Value): Value =
    store.computeIfAbsent(key, acquire(_))

  override def invalidateAll(): Unit = store.clear()
}
