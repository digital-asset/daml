// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache
import java.time.Instant

import com.daml.caching.SizedCache
import com.daml.lf.data.Ref.Party
import com.daml.lf.value.Value.{ContractId, ContractInst, VersionedValue}
import com.daml.metrics.Metrics

import scala.concurrent.ExecutionContext

object ContractsStateCache {
  private[cache] type Value = VersionedValue[ContractId]
  private[cache] type Contract = ContractInst[Value]

  def apply(cacheSize: Long, metrics: Metrics)(implicit
      ec: ExecutionContext
  ): StateCache[ContractId, ContractStateValue] =
    StateCache(
      cache = SizedCache.from[ContractId, ContractStateValue](
        SizedCache.Configuration(cacheSize),
        metrics.daml.execution.contractStateCache,
      ),
      metrics = metrics,
    )

  // Possible lifecycle cache values of a contract
  sealed trait ContractStateValue extends Product with Serializable
  object ContractStateValue {
    final case object NotFound extends ContractStateValue

    sealed trait ExistingContractValue extends ContractStateValue

    final case class Active(
        contract: Contract,
        stakeholders: Set[Party],
        createLedgerEffectiveTime: Instant,
    ) extends ExistingContractValue

    final case class Archived(stakeholders: Set[Party]) extends ExistingContractValue
  }
}
