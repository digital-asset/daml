// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

final class NoCache[Key, Value] private[caching] extends Cache[Key, Value] {
  override def put(key: Key, value: Value): Unit = ()

  override def get(key: Key, acquire: Key => Value): Value = acquire(key)

  override def getIfPresent(key: Key): Option[Value] = None
}
