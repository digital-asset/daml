// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.digitalasset.canton.caching.SizedCache
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics

import scala.concurrent.ExecutionContext

object ContractsStateCache {
  def apply(
      initialCacheIndex: Offset,
      cacheSize: Long,
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): StateCache[ContractId, ContractStateValue] =
    StateCache(
      initialCacheIndex = initialCacheIndex,
      cache = SizedCache.from[ContractId, ContractStateValue](
        SizedCache.Configuration(cacheSize),
        metrics.execution.cache.contractState.stateCache,
      ),
      registerUpdateTimer = metrics.execution.cache.contractState.registerCacheUpdate,
      loggerFactory = loggerFactory,
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
      signatories: Set[Party],
      globalKey: Option[GlobalKey],
      keyMaintainers: Option[Set[Party]],
      driverMetadata: Option[Array[Byte]],
  ) extends ExistingContractValue

  final case class Archived(stakeholders: Set[Party]) extends ExistingContractValue
}
