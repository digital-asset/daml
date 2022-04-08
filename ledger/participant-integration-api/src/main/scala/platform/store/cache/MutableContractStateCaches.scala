// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package platform.store.cache

import lf.transaction.GlobalKey
import logging.{ContextualizedLogger, LoggingContext}
import metrics.Metrics
import platform.store.appendonlydao.events.ContractStateEvent
import platform.store.appendonlydao.events.ContractStateEvent.LedgerEndMarker
import platform.store.cache.ContractKeyStateValue.{Assigned, Unassigned}
import platform.store.cache.ContractStateValue.{Active, Archived}

import scala.concurrent.ExecutionContext

class MutableContractStateCaches(
    val keyState: StateCache[GlobalKey, ContractKeyStateValue],
    val contractState: StateCache[ContractId, ContractStateValue],
) {
  private val logger = ContextualizedLogger.get(getClass)

  def pushBatch(events: Seq[ContractStateEvent])(implicit loggingContext: LoggingContext): Unit = {
    events.foreach(debugEvents)
    batchUpdateCaches(events)
  }

  private def batchUpdateCaches(
      events: Seq[ContractStateEvent]
  )(implicit loggingContext: LoggingContext): Unit = {
    val (keysO, contractsO) = events.map {
      case ContractStateEvent.Created(
            contractId,
            contract,
            globalKey,
            createLedgerEffectiveTime,
            flatEventWitnesses,
            _,
            _,
          ) =>
        globalKey.map(_ -> Assigned(contractId, flatEventWitnesses)) -> Some(
          contractId -> Active(contract, flatEventWitnesses, createLedgerEffectiveTime)
        )
      case ContractStateEvent.Archived(
            contractId,
            globalKey,
            stakeholders,
            _,
            _,
          ) =>
        globalKey.map(_ -> Unassigned) -> Some(contractId -> Archived(stakeholders))
      case _: LedgerEndMarker => None -> None
    }.unzip

    keyState.putBatch(events.head.eventOffset, keysO.iterator.flatten.toSeq)
    contractState.putBatch(events.head.eventOffset, contractsO.iterator.flatten.toSeq)
  }

  private def debugEvents(
      event: ContractStateEvent
  )(implicit loggingContext: LoggingContext): Unit =
    event match {
      case ContractStateEvent.Created(
            contractId,
            _,
            globalKey,
            _,
            _,
            eventOffset,
            eventSequentialId,
          ) =>
        logger.debug(
          s"State events update: Created(contractId=${contractId.coid}, globalKey=$globalKey, offset=$eventOffset, eventSequentialId=$eventSequentialId)"
        )
      case ContractStateEvent.Archived(
            contractId,
            globalKey,
            _,
            eventOffset,
            eventSequentialId,
          ) =>
        logger.debug(
          s"State events update: Archived(contractId=${contractId.coid}, globalKey=$globalKey, offset=$eventOffset, eventSequentialId=$eventSequentialId)"
        )
      case LedgerEndMarker(eventOffset, eventSequentialId) =>
        logger.debug(
          s"Ledger end reached: $eventOffset -> $eventSequentialId"
        )
    }
}

object MutableContractStateCaches {
  def build(
      maxContractsCacheSize: Long,
      maxKeyCacheSize: Long,
      metrics: Metrics,
  )(implicit ec: ExecutionContext): MutableContractStateCaches =
    new MutableContractStateCaches(
      contractState = ContractsStateCache(maxContractsCacheSize, metrics),
      keyState = ContractKeyStateCache(maxKeyCacheSize, metrics),
    )
}
