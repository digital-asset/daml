// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.canton.caching.SizedCache
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics

import scala.concurrent.ExecutionContext

object ContractKeyStateCache {
  def apply(
      initialCacheIndex: Offset,
      cacheSize: Long,
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): StateCache[GlobalKey, ContractKeyStateValue] =
    StateCache(
      initialCacheIndex = initialCacheIndex,
      cache = SizedCache.from[GlobalKey, ContractKeyStateValue](
        SizedCache.Configuration(cacheSize),
        metrics.execution.cache.keyState.stateCache,
      ),
      registerUpdateTimer = metrics.execution.cache.keyState.registerCacheUpdate,
      loggerFactory = loggerFactory,
    )
}

sealed trait ContractKeyStateValue extends Product with Serializable

object ContractKeyStateValue {

  final case class Assigned(contractId: ContractId, createWitnesses: Set[Party])
      extends ContractKeyStateValue

  final case object Unassigned extends ContractKeyStateValue
}
