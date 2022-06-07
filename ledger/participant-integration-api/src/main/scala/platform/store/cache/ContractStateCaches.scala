// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.ledger.offset.Offset
import com.daml.lf.transaction.GlobalKey
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.ContractKeyStateValue.{Assigned, Unassigned}
import com.daml.platform.store.cache.ContractStateValue.{Active, Archived, ExistingContractValue}
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.dao.events.ContractStateEvent.LedgerEndMarker

import scala.concurrent.ExecutionContext

class ContractStateCaches(
    private[cache] val keyState: StateCache[GlobalKey, ContractKeyStateValue],
    private[cache] val contractState: StateCache[ContractId, ContractStateValue],
)(implicit loggingContext: LoggingContext) {
  private val logger = ContextualizedLogger.get(getClass)

  def push(eventsBatch: Vector[ContractStateEvent]): Unit =
    if (eventsBatch.isEmpty) {
      logger.error("push triggered with empty events batch")
    } else {
      val keyMappingsBuilder = Map.newBuilder[Key, ContractKeyStateValue]
      val contractMappingsBuilder = Map.newBuilder[ContractId, ExistingContractValue]

      eventsBatch.foreach {
        case created: ContractStateEvent.Created =>
          created.globalKey.foreach { key =>
            keyMappingsBuilder.addOne(key -> Assigned(created.contractId, created.stakeholders))
          }
          contractMappingsBuilder.addOne(
            created.contractId,
            Active(created.contract, created.stakeholders, created.ledgerEffectiveTime),
          )
        case archived: ContractStateEvent.Archived =>
          archived.globalKey.foreach { key =>
            keyMappingsBuilder.addOne(key -> Unassigned)
          }
          contractMappingsBuilder.addOne(archived.contractId, Archived(archived.stakeholders))
        case _: LedgerEndMarker => ()
      }

      val keyMappings = keyMappingsBuilder.result()
      val contractMappings = contractMappingsBuilder.result()

      val validAt = eventsBatch.last.eventOffset
      if (keyMappings.nonEmpty) keyState.putBatch(validAt, keyMappings)
      if (contractMappings.nonEmpty)
        contractState.putBatch(validAt, contractMappings)
    }
}

object ContractStateCaches {
  def build(
      initialCacheIndex: Offset,
      maxContractsCacheSize: Long,
      maxKeyCacheSize: Long,
      metrics: Metrics,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ContractStateCaches =
    new ContractStateCaches(
      contractState = ContractsStateCache(initialCacheIndex, maxContractsCacheSize, metrics),
      keyState = ContractKeyStateCache(initialCacheIndex, maxKeyCacheSize, metrics),
    )
}
