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
      contractKeyHash: Option[
        String
      ], // TODO this is not really needed, only for being able to resolve hash conflicts. Hash conflicts would still be there later too, if we are truncating the key-hash, but they should be resolved in the engine instead with non-UCKs
  ) extends ExistingContractValue

  final case class Archived(
      stakeholders: Set[Party],
      contractKeyHash: Option[
        String
      ], // TODO this is not really needed, only for being able to resolve hash conflicts. Hash conflicts would still be there later too, if we are truncating the key-hash, but they should be resolved in the engine instead with non-UCKs
  ) extends ExistingContractValue
}
