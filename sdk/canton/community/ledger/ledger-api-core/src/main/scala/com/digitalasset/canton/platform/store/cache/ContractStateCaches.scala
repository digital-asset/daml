// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import cats.data.NonEmptyVector
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.{
  Active,
  Archived,
  ExistingContractStatus,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.cache.ContractKeyStateValue.{Assigned, Unassigned}
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent.ReassignmentAccepted
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.transaction.GlobalKey

import scala.concurrent.ExecutionContext

/** Encapsulates the contract and key state caches with operations for mutating them. The caches are
  * used for serving contract activeness and key lookups for command interpretation performed during
  * command submission.
  *
  * @param keyState
  *   The contract key state cache.
  * @param contractState
  *   The contract state cache.
  * @param loggerFactory
  *   The logger factory.
  */
class ContractStateCaches(
    private[cache] val keyState: StateCache[GlobalKey, ContractKeyStateValue],
    private[cache] val contractState: StateCache[ContractId, ContractStateStatus],
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Update the state caches with a batch of events.
    *
    * @param eventsBatch
    *   The contract state update events batch. The updates batch must be non-empty and with
    *   strictly increasing event sequential ids.
    */
  def push(
      eventsBatch: NonEmptyVector[ContractStateEvent],
      lastEventSeqId: Long,
  )(implicit traceContext: TraceContext): Unit = {
    val keyMappingsBuilder = Map.newBuilder[Key, ContractKeyStateValue]
    val contractMappingsBuilder = Map.newBuilder[ContractId, ExistingContractStatus]

    eventsBatch.toVector.foreach {
      case created: ContractStateEvent.Created =>
        created.globalKey.foreach(key =>
          keyMappingsBuilder.addOne(
            key -> Assigned(created.contractId)
          )
        )
        contractMappingsBuilder.addOne(created.contractId -> Active)

      case archived: ContractStateEvent.Archived =>
        archived.globalKey.foreach { key =>
          keyMappingsBuilder.addOne(key -> Unassigned)
        }
        contractMappingsBuilder.addOne(archived.contractId -> Archived)

      case ReassignmentAccepted => ()
    }

    val keyMappings = keyMappingsBuilder.result()
    val contractMappings = contractMappingsBuilder.result()

    val validAt = lastEventSeqId
    keyState.putBatch(validAt, keyMappings)
    contractState.putBatch(validAt, contractMappings)
  }

  /** Reset the contract and key state caches to the specified offset. */
  def reset(lastPersistedLedgerEnd: Option[LedgerEnd]): Unit = {
    val index = lastPersistedLedgerEnd.map(_.lastEventSeqId).getOrElse(0L)
    keyState.reset(index)
    contractState.reset(index)
  }
}

object ContractStateCaches {
  def build(
      initialCacheEventSeqIdIndex: Long,
      maxContractsCacheSize: Long,
      maxKeyCacheSize: Long,
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): ContractStateCaches =
    new ContractStateCaches(
      contractState = ContractsStateCache(
        initialCacheEventSeqIdIndex,
        maxContractsCacheSize,
        metrics,
        loggerFactory,
      ),
      keyState =
        ContractKeyStateCache(initialCacheEventSeqIdIndex, maxKeyCacheSize, metrics, loggerFactory),
      loggerFactory = loggerFactory,
    )
}
