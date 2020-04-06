// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.caching

sealed trait Configuration[Key, Value] {
  def build: Cache[Key, Value]
}

object Configuration {

  final class NoCache[Key, Value] extends Configuration[Key, Value] {
    override def build: Cache[Key, Value] = Cache.none
  }

  final class MaxWeight[Key <: AnyRef: Weight, Value <: AnyRef: Weight](weight: Size)
      extends Configuration[Key, Value] {
    override def build: Cache[Key, Value] = Cache.maxWeight(weight)
  }

}
