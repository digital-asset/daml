// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching
import com.daml.ledger.participant.state.kvutils.DamlKvutils

/**
  * Cache write policy that enables write-through caching, i.e., allows caching of all committed keys
  * and allows caching all read keys.
  * This policy should be used for ledgers that guarantee serializability of transactions.
  */
object AlwaysCacheUpdatePolicy extends CacheUpdatePolicy {
  override def shouldCacheOnWrite(key: DamlKvutils.DamlStateKey): Boolean = true

  override def shouldCacheOnRead(key: DamlKvutils.DamlStateKey): Boolean = true
}
