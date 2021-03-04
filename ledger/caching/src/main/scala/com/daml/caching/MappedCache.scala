// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

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

  override val cache: Null = null // TDT
}
