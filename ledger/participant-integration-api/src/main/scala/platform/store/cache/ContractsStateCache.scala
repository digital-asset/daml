// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache
import com.daml.caching.SizedCache
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics

import scala.concurrent.ExecutionContext

object ContractsStateCache {
  def apply(cacheSize: Long, metrics: Metrics)(implicit
      ec: ExecutionContext
  ): StateCache[ContractId, ContractStateValue] =
    StateCache(
      cache = SizedCache.from[ContractId, ContractStateValue](
        SizedCache.Configuration(cacheSize),
        metrics.daml.execution.cache.contractState,
      ),
      registerUpdateTimer = metrics.daml.execution.cache.registerCacheUpdate,
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
