// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

private[caching] final class MappedCache[Key, Value, NewValue](
    mapAfterReading: Value => NewValue,
    mapBeforeWriting: NewValue => Option[Value],
)(delegate: Cache[Key, Value])
    extends Cache[Key, NewValue] {
  override def put(key: Key, value: NewValue): Unit =
    mapBeforeWriting(value).foreach { cacheValue =>
      delegate.put(key, cacheValue)
    }

  override def getIfPresent(key: Key): Option[NewValue] =
    delegate.getIfPresent(key).map(mapAfterReading)

  override def putAll(mappings: Map[Key, NewValue]): Unit = {
    val mappedBuilder = Map.newBuilder[Key, Value]

    mappings.foreach { case (key, value) =>
      mapBeforeWriting(value)
        .foreach(newValue => mappedBuilder.addOne(key -> newValue))
    }

    delegate.putAll(mappedBuilder.result())
  }

  override def invalidateAll(): Unit = delegate.invalidateAll()
}
