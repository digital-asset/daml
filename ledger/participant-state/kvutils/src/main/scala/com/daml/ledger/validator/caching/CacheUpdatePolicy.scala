// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

trait CacheUpdatePolicy[-Key] {
  def shouldCacheOnWrite(key: Key): Boolean

  def shouldCacheOnRead(key: Key): Boolean
}
