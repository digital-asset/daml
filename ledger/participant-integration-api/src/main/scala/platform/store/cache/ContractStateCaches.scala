// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.backend.PersistentContractKeyHash
import com.daml.platform.store.cache.ContractStateValue.{Active, Archived, ExistingContractValue}
import com.daml.platform.store.dao.events.ContractStateEvent

import scala.concurrent.ExecutionContext

/** Encapsulates the contract and key state caches with operations for mutating them.
  * The caches are used for serving contract activeness and key lookups
  * for command interpretation performed during command submission.
  *
  * @param keyState The contract key state cache.
  * @param contractState The contract state cache.
  * @param loggingContext The logging context.
  */
class ContractStateCaches(
    private[cache] val keyState: ContractKeyStateCache,
    private[cache] val contractState: StateCache[ContractId, ContractStateValue],
)(implicit loggingContext: LoggingContext) {
  private val logger = ContextualizedLogger.get(getClass)

  /** Update the state caches with a batch of events.
    *
    * @param eventsBatch The contract state update events batch.
    *                    The updates batch must be non-empty and with strictly increasing event sequential ids.
    */
  def push(eventsBatch: Vector[ContractStateEvent]): Unit =
    if (eventsBatch.isEmpty) {
      logger.error("push triggered with empty events batch")
    } else {
      val newKeyMappingsBuilder = Vector.newBuilder[(Long, ContractId)]
      val removedKeyMappingsBuilder = Vector.newBuilder[(Long, ContractId)]
      val contractMappingsBuilder = Map.newBuilder[ContractId, ExistingContractValue]

      eventsBatch.foreach {
        case created: ContractStateEvent.Created =>
          created.globalKey.foreach { key =>
            newKeyMappingsBuilder.addOne(PersistentContractKeyHash(key) -> created.contractId)
          }
          contractMappingsBuilder.addOne(
            created.contractId,
            Active(
              created.contract,
              created.stakeholders,
              created.ledgerEffectiveTime,
              created.globalKey.map(_.hash.bytes.toHexString),
            ),
          )
        case archived: ContractStateEvent.Archived =>
          archived.globalKey.foreach { key =>
            removedKeyMappingsBuilder.addOne(PersistentContractKeyHash(key) -> archived.contractId)
          }
          contractMappingsBuilder.addOne(
            archived.contractId,
            Archived(archived.stakeholders, archived.globalKey.map(_.hash.bytes.toHexString)),
          )
      }

      val newKeyMappings = newKeyMappingsBuilder.result()
      val removedKeyMappings = removedKeyMappingsBuilder.result()
      val contractMappings = contractMappingsBuilder.result()

      val validAt = eventsBatch.last.eventOffset
      if (newKeyMappings.nonEmpty || removedKeyMappings.nonEmpty) {
        keyState.putBatch(validAt, newKeyMappings, removedKeyMappings)
      }
      contractState.putBatch(validAt, contractMappings)
    }

  /** Reset the contract and key state caches to the specified offset. */
  def reset(lastPersistedLedgerEnd: Offset): Unit = {
    keyState.reset(lastPersistedLedgerEnd)
    contractState.reset(lastPersistedLedgerEnd)
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
