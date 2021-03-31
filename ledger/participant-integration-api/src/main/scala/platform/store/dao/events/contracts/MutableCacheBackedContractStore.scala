// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events.contracts

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink}
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractInst, VersionedValue}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.cache.ContractsStateCache.ContractStateValue._
import com.daml.platform.store.cache.ContractsStateCache._
import com.daml.platform.store.cache.KeyStateCache.KeyStateValue
import com.daml.platform.store.cache.KeyStateCache.KeyStateValue._
import com.daml.platform.store.cache.{ContractsStateCache, KeyStateCache, StateCache}
import com.daml.platform.store.dao.events
import com.daml.platform.store.dao.events.ContractId
import com.daml.platform.store.dao.events.contracts.ContractStateEvent.LedgerEndMarker
import com.daml.platform.store.dao.events.contracts.LedgerDaoContractsReader.{
  ActiveContract,
  ArchivedContract,
  ContractState,
  KeyState,
}
import com.daml.platform.store.dao.events.contracts.MutableCacheBackedContractStore._

import scala.concurrent.{ExecutionContext, Future}

class MutableCacheBackedContractStore(
    metrics: Metrics,
    contractsReader: LedgerDaoContractsReader,
    signalNewLedgerHead: Offset => Unit,
    private[contracts] val keyCache: StateCache[GlobalKey, KeyStateValue],
    private[contracts] val contractsCache: StateCache[ContractId, ContractStateValue],
)(implicit
    executionContext: ExecutionContext
) extends ContractStore {
  private val logger = ContextualizedLogger.get(getClass)
  private[contracts] val cacheOffset = new AtomicLong(0L)

  def consumeFrom(implicit
      loggingContext: LoggingContext
  ): Flow[ContractStateEvent, Unit, NotUsed] =
    Flow[ContractStateEvent]
      .wireTap(debugEvents(_))
      .alsoTo(Sink.foreach(updateCaches))
      .map(updateOffsets)

  override def lookupActiveContract(readers: Set[Party], contractId: Value.ContractId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractInst[VersionedValue[ContractId]]]] =
    contractsCache
      .get(contractId)
      .map(contractStateToResponse(_, readers, contractId))
      .getOrElse(readThroughStateCache(contractId, readers))

  override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Value.ContractId]] =
    keyCache
      .get(key)
      .map(keyStateToResponse(_, readers))
      .map(Future.successful)
      .getOrElse(readThroughKeyCache(key, readers))

  override def lookupMaximumLedgerTime(ids: Set[Value.ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Instant]] =
    if (ids.isEmpty) Future.failed(EmptyContractIds())
    else
      for {
        (cached, toBeFetched) <- collectActive(ids)
        max <-
          if (toBeFetched.isEmpty)
            Future.successful(Some(cached.max))
          else
            contractsReader
              .lookupMaximumLedgerTime(toBeFetched)
              .map(_.map(m => (cached + m).max))
      } yield max

  private def collectActive(
      ids: Set[events.ContractId]
  )(implicit loggingContext: LoggingContext) = Future {
    val cacheQueried = ids.map(id => id -> contractsCache.get(id))
    val cached = cacheQueried.collect {
      case (_, Some(Active(_, _, createLedgerEffectiveTime))) => createLedgerEffectiveTime
      case (_, Some(_)) => throw ContractNotFound(ids)
    }
    val missing = cacheQueried.collect { case (id, None) => id }
    (cached, missing)
  }

  private def readThroughStateCache(contractId: ContractId, readers: Set[Party])(implicit
      loggingContext: LoggingContext
  ) = {
    val currentCacheOffset = cacheOffset.get()
    val fetchStateRequest =
      Timed.future(
        metrics.daml.index.lookupContract,
        contractsReader.lookupContractState(contractId, currentCacheOffset),
      )
    val cacheValueUpdate = fetchStateRequest map toContractCacheValue

    contractsCache.putAsync(
      key = contractId,
      validAt = currentCacheOffset,
      eventualValue = cacheValueUpdate,
    )

    cacheValueUpdate.flatMap(contractStateToResponse(_, readers, contractId))
  }

  private def keyStateToResponse(
      value: KeyStateValue,
      readers: Set[Party],
  ): Option[ContractId] = value match {
    case Assigned(contractId, createWitnesses)
        if `intersection non-empty`(readers, createWitnesses) =>
      Some(contractId)
    case _ => Option.empty
  }

  private def contractStateToResponse(
      value: ContractStateValue,
      readers: Set[Party],
      contractId: ContractId,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractInst[VersionedValue[ContractId]]]] =
    value match {
      case Active(contract, stakeholders, _) if `intersection non-empty`(stakeholders, readers) =>
        Future.successful(Some(contract))
      case Archived(stakeholders) if `intersection non-empty`(stakeholders, readers) =>
        Future.successful(Option.empty)
      case ContractStateValue.NotFound =>
        logger.warn(s"Contract not found for $contractId")
        Future.successful(Option.empty)
      case existingContractValue: ExistingContractValue =>
        logger.debug(s"Checking divulgence for contractId=$contractId and readers=$readers")
        resolveDivulgenceLookup(existingContractValue, contractId, readers)
    }

  private def resolveDivulgenceLookup(
      existingContractValue: ExistingContractValue,
      contractId: ContractId,
      forParties: Set[Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractInst[VersionedValue[ContractId]]]] = {
    existingContractValue match {
      case Active(contract, _, _) =>
        contractsReader.lookupActiveContractWithCachedArgument(
          contractId,
          forParties,
          contract.arg,
        )
      case _: Archived =>
        // We need to fetch the contract here since the archival
        // may have not been divulged to the readers
        contractsReader.lookupActiveContractAndLoadArgument(
          contractId,
          forParties,
        )
    }
  }

  private def toContractCacheValue(state: Option[ContractState]): ContractStateValue =
    state
      .map {
        case ActiveContract(contract, stakeholders, ledgerEffectiveTime) =>
          ContractStateValue.Active(contract, stakeholders, ledgerEffectiveTime)
        case ArchivedContract(stakeholders) =>
          ContractStateValue.Archived(stakeholders)
      }
      .getOrElse(ContractStateValue.NotFound)

  private def toKeyCacheValue(state: KeyState): KeyStateValue =
    state match {
      case LedgerDaoContractsReader.KeyAssigned(contractId, stakeholders) =>
        Assigned(contractId, stakeholders)
      case LedgerDaoContractsReader.KeyUnassigned =>
        Unassigned
    }

  private def readThroughKeyCache(
      key: GlobalKey,
      readers: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[ContractId]] = {
    val currentCacheOffset = cacheOffset.get()
    val eventualResult = contractsReader.lookupKeyState(key, currentCacheOffset)
    val cacheStateUpdate = eventualResult map toKeyCacheValue

    keyCache.putAsync(
      key,
      currentCacheOffset,
      cacheStateUpdate,
    )

    cacheStateUpdate.map(keyStateToResponse(_, readers))
  }

  private def `intersection non-empty`[T](one: Set[T], other: Set[T]): Boolean =
    one.intersect(other).nonEmpty

  private def updateOffsets(event: ContractStateEvent): Unit = {
    cacheOffset.set(event.eventSequentialId)
    metrics.daml.indexer.currentStateCacheSequentialIdGauge.updateValue(event.eventSequentialId)
    event match {
      case LedgerEndMarker(eventOffset, _) => signalNewLedgerHead(eventOffset)
      case _ => ()
    }
  }

  private def updateCaches(event: ContractStateEvent): Unit =
    event match {
      case ContractStateEvent.Created(
            contractId,
            contract,
            globalKey,
            createLedgerEffectiveTime,
            flatEventWitnesses,
            _,
            eventSequentialId,
          ) =>
        globalKey.foreach(
          keyCache.put(_, eventSequentialId, Assigned(contractId, flatEventWitnesses))
        )
        contractsCache.put(
          contractId,
          eventSequentialId,
          Active(contract, flatEventWitnesses, createLedgerEffectiveTime),
        )
      case ContractStateEvent.Archived(
            contractId,
            globalKey,
            stakeholders,
            _,
            eventSequentialId,
          ) =>
        globalKey.foreach(keyCache.put(_, eventSequentialId, Unassigned))
        contractsCache.put(
          contractId,
          eventSequentialId,
          Archived(stakeholders),
        )
      case _ => ()
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
          s"State events update: Created(contractId=$contractId, globalKey=$globalKey, offset=$eventOffset, eventSequentialId=$eventSequentialId"
        )
      case ContractStateEvent.Archived(
            contractId,
            globalKey,
            _,
            eventOffset,
            eventSequentialId,
          ) =>
        logger.debug(
          s"State events update: Archived(contractId=$contractId, globalKey=$globalKey, offset=$eventOffset, eventSequentialId=$eventSequentialId"
        )
      case LedgerEndMarker(eventOffset, eventSequentialId) =>
        logger.debug(
          s"Ledger end reached: $eventOffset -> $eventSequentialId "
        )
    }
}

object MutableCacheBackedContractStore {
  type EventSequentialId = Long
  def apply(
      contractsReader: LedgerDaoContractsReader,
      signalNewLedgerHead: Offset => Unit,
      metrics: Metrics,
      contractsCacheSize: Long = 100000L,
      keyCacheSize: Long = 100000L,
  )(implicit
      executionContext: ExecutionContext
  ): MutableCacheBackedContractStore =
    new MutableCacheBackedContractStore(
      metrics,
      contractsReader,
      signalNewLedgerHead,
      KeyStateCache(keyCacheSize, metrics),
      ContractsStateCache(contractsCacheSize, metrics),
    )

  final case class ContractNotFound(contractIds: Set[ContractId])
      extends IllegalArgumentException(
        s"One or more of the following contract identifiers has been found: ${contractIds.map(_.coid).mkString(", ")}"
      )
  final case class EmptyContractIds()
      extends IllegalArgumentException(
        "Cannot lookup the maximum ledger time for an empty set of contract identifiers"
      )
}
