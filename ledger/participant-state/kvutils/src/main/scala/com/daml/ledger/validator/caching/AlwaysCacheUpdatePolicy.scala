// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

/**
  * Cache update policy that enables write-through caching, i.e., allows caching of all committed keys
  * and allows caching all read keys.
  * This policy should be used for ledgers that guarantee serializability of transactions.
  */
object AlwaysCacheUpdatePolicy extends CacheUpdatePolicy[Any] {
  override def shouldCacheOnWrite(key: Any): Boolean = true

  override def shouldCacheOnRead(key: Any): Boolean = true
}
