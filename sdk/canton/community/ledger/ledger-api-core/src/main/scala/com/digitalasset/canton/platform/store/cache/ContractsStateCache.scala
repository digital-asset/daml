// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.caching.SizedCache
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics

import scala.concurrent.ExecutionContext

object ContractsStateCache {
  def apply(
      initialCacheEventSeqIdIndex: Long,
      cacheSize: Long,
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): StateCache[ContractId, ContractStateStatus] =
    StateCache(
      initialCacheEventSeqIdIndex = initialCacheEventSeqIdIndex,
      emptyLedgerState = ContractStateStatus.NotFound,
      cache = SizedCache.from[ContractId, ContractStateStatus](
        SizedCache.Configuration(cacheSize),
        metrics.execution.cache.contractState.stateCache,
      ),
      registerUpdateTimer = metrics.execution.cache.contractState.registerCacheUpdate,
      loggerFactory = loggerFactory,
    )
}
