// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.caching.SizedCache
import com.daml.ledger.offset.Offset
import com.daml.lf.transaction.GlobalKey
import com.daml.metrics.Metrics

import scala.concurrent.ExecutionContext

object ContractKeyStateCache {
  def apply(initialCacheIndex: Offset, cacheSize: Long, metrics: Metrics)(implicit
      ec: ExecutionContext
  ): StateCache[GlobalKey, ContractKeyStateValue] =
    new StateCache(
      initialCacheIndex = initialCacheIndex,
      cache = SizedCache.from[GlobalKey, ContractKeyStateValue](
        SizedCache.Configuration(cacheSize),
        metrics.daml.execution.cache.keyState.stateCache,
      ),
      registerUpdateTimer = metrics.daml.execution.cache.keyState.registerCacheUpdate,
    )
}

sealed trait ContractKeyStateValue extends Product with Serializable

object ContractKeyStateValue {

  final case class Assigned(contractId: ContractId, createWitnesses: Set[Party])
      extends ContractKeyStateValue

  final case object Unassigned extends ContractKeyStateValue
}
