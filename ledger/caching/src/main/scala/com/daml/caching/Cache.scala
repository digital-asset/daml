// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

abstract class Cache[Key, Value] {
  def put(key: Key, value: Value): Unit

  def get(key: Key, acquire: Key => Value): Value

  def getIfPresent(key: Key): Option[Value]
}

object Cache {

  type Size = Long

  def none[Key, Value]: Cache[Key, Value] = new NoCache

}
