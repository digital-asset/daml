// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.caching.SizedCache
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics

import scala.concurrent.ExecutionContext

object ContractsStateCache {
  def apply(initialCacheIndex: Offset, cacheSize: Long, metrics: Metrics)(implicit
      ec: ExecutionContext
  ): StateCache[ContractId, ContractStateValue] =
    new StateCache(
      initialCacheIndex = initialCacheIndex,
      cache = SizedCache.from[ContractId, ContractStateValue](
        SizedCache.Configuration(cacheSize),
        metrics.daml.execution.cache.contractState.stateCache,
      ),
      registerUpdateTimer = metrics.daml.execution.cache.contractState.registerCacheUpdate,
    )
}

sealed trait ContractStateValue extends Product with Serializable

object ContractStateValue {
  final case object NotFound extends ContractStateValue

  sealed trait ExistingContractValue extends ContractStateValue

  final case class Active(
      contract: Contract,
      stakeholders: Set[Party],
      createLedgerEffectiveTime: Timestamp,
  ) extends ExistingContractValue

  final case class Archived(stakeholders: Set[Party]) extends ExistingContractValue
}
