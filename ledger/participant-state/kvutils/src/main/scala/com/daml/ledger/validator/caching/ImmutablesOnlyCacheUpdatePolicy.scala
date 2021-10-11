// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching
import com.daml.ledger.participant.state.kvutils.store.DamlStateKey

/** Cache update policy that allows writing to-be-committed packages back to the cache and read
  * immutable values such as packages and party allocations from the ledger.
  * This policy should be used for ledgers with multiple committers.
  * In case a commit fails and hence does not get persisted e.g. because of a conflicting write set
  * with another commit, this policy ensures that we don't need to invalidate items in the cache.
  * Specifically, packages are content-addressed hence may be cached without consistency issues,
  * whereas party allocations must be only cached once they are available on the ledger.
  */
object ImmutablesOnlyCacheUpdatePolicy extends CacheUpdatePolicy[DamlStateKey] {
  override def shouldCacheOnWrite(key: DamlStateKey): Boolean =
    key.getPackageId.nonEmpty

  override def shouldCacheOnRead(key: DamlStateKey): Boolean =
    key.getPackageId.nonEmpty || key.getParty.nonEmpty
}
