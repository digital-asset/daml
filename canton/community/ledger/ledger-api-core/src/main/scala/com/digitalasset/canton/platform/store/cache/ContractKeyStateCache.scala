// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.lf.transaction.GlobalKey
import com.digitalasset.canton.caching.SizedCache
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.Metrics

import scala.concurrent.ExecutionContext

object ContractKeyStateCache {
  def apply(
      initialCacheIndex: Offset,
      cacheSize: Long,
      metrics: Metrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): StateCache[GlobalKey, ContractKeyStateValue] =
    StateCache(
      initialCacheIndex = initialCacheIndex,
      cache = SizedCache.from[GlobalKey, ContractKeyStateValue](
        SizedCache.Configuration(cacheSize),
        metrics.daml.execution.cache.keyState.stateCache,
      ),
      registerUpdateTimer = metrics.daml.execution.cache.keyState.registerCacheUpdate,
      loggerFactory = loggerFactory,
    )
}

sealed trait ContractKeyStateValue extends Product with Serializable

object ContractKeyStateValue {

  final case class Assigned(contractId: ContractId, createWitnesses: Set[Party])
      extends ContractKeyStateValue

  final case object Unassigned extends ContractKeyStateValue
}
