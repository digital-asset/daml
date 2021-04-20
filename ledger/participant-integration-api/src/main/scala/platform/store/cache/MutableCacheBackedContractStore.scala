// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.stream.{KillSwitches, Materializer, RestartSettings, UniqueKillSwitch}
import akka.{Done, NotUsed}
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.resources.Resource
import com.daml.lf.transaction.GlobalKey
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.appendonlydao.EventSequentialId
import com.daml.platform.store.cache.ContractKeyStateValue._
import com.daml.platform.store.cache.ContractStateValue._
import com.daml.platform.store.cache.MutableCacheBackedContractStore._
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.dao.events.ContractStateEvent.LedgerEndMarker
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.{
  ActiveContract,
  ArchivedContract,
  ContractState,
  KeyState,
}
import com.daml.scalautil.Statement.discard

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}

class MutableCacheBackedContractStore(
    metrics: Metrics,
    contractsReader: LedgerDaoContractsReader,
    signalNewLedgerHead: SignalNewLedgerHead,
    subscribeToContractStateEvents: SubscribeToContractStateEvents,
    private[cache] val keyCache: StateCache[GlobalKey, ContractKeyStateValue],
    private[cache] val contractsCache: StateCache[ContractId, ContractStateValue],
    minBackoffStreamRestart: FiniteDuration,
)(implicit mat: Materializer, executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends ContractStore
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(getClass)

  private[cache] val cacheIndex = new CacheIndex()

  private val (contractStateUpdateKillSwitch, contractStateUpdateDone) =
    RestartSource
      .withBackoff(
        RestartSettings(
          minBackoff = minBackoffStreamRestart,
          maxBackoff = 10.seconds,
          randomFactor = 0.2,
        )
      )(() =>
        subscribeToContractStateEvents
          .tupled(cacheIndex.get)
          .wireTap(debugEvents(_))
          .alsoTo(Sink.foreach(updateCaches))
          .map(updateOffsets)
      )
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(Sink.ignore)(Keep.both[UniqueKillSwitch, Future[Done]])
      .run()

  override def lookupActiveContract(readers: Set[Party], contractId: ContractId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Contract]] =
    contractsCache
      .get(contractId)
      .map(Future.successful)
      .getOrElse(readThroughContractsCache(contractId))
      .flatMap(contractStateToResponse(readers, contractId))

  override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    keyCache
      .get(key)
      .map(Future.successful)
      .getOrElse(readThroughKeyCache(key))
      .map(keyStateToResponse(_, readers))

  override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Instant]] =
    if (ids.isEmpty)
      Future.failed(EmptyContractIds())
    else {
      val (cached, toBeFetched) = partitionCached(ids)
      if (toBeFetched.isEmpty)
        Future.successful(Some(cached.max))
      else
        contractsReader
          .lookupMaximumLedgerTime(toBeFetched)
          .map(_.map(m => (cached + m).max))
    }

  private def partitionCached(
      ids: Set[ContractId]
  )(implicit loggingContext: LoggingContext) = {
    val cacheQueried = ids.map(id => id -> contractsCache.get(id))
    val cached = cacheQueried.collect {
      case (_, Some(Active(_, _, createLedgerEffectiveTime))) => createLedgerEffectiveTime
      case (_, Some(_)) => throw ContractNotFound(ids)
    }
    val missing = cacheQueried.collect { case (id, None) => id }
    (cached, missing)
  }

  private def readThroughContractsCache(contractId: ContractId)(implicit
      loggingContext: LoggingContext
  ) = {
    val currentCacheSequentialId = cacheIndex.getSequentialId
    val fetchStateRequest =
      Timed.future(
        metrics.daml.index.lookupContract,
        contractsReader.lookupContractState(contractId, currentCacheSequentialId),
      )
    val eventualValue = fetchStateRequest.map(toContractCacheValue)

    for {
      _ <- contractsCache.putAsync(
        key = contractId,
        validAt = currentCacheSequentialId,
        eventualValue = eventualValue,
      )
      value <- eventualValue
    } yield value
  }

  private def keyStateToResponse(
      value: ContractKeyStateValue,
      readers: Set[Party],
  ): Option[ContractId] = value match {
    case Assigned(contractId, createWitnesses) if nonEmptyIntersection(readers, createWitnesses) =>
      Some(contractId)
    case _: Assigned | Unassigned => Option.empty
  }

  private def contractStateToResponse(readers: Set[Party], contractId: ContractId)(
      value: ContractStateValue
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[Contract]] =
    value match {
      case Active(contract, stakeholders, _) if nonEmptyIntersection(stakeholders, readers) =>
        Future.successful(Some(contract))
      case Archived(stakeholders) if nonEmptyIntersection(stakeholders, readers) =>
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
  ): Future[Option[Contract]] =
    existingContractValue match {
      case Active(contract, _, _) =>
        contractsReader.lookupActiveContractWithCachedArgument(
          forParties,
          contractId,
          contract.arg,
        )
      case _: Archived =>
        // We need to fetch the contract here since the archival
        // may have not been divulged to the readers
        contractsReader.lookupActiveContractAndLoadArgument(
          forParties,
          contractId,
        )
    }

  private val toContractCacheValue: Option[ContractState] => ContractStateValue = {
    case Some(ActiveContract(contract, stakeholders, ledgerEffectiveTime)) =>
      ContractStateValue.Active(contract, stakeholders, ledgerEffectiveTime)
    case Some(ArchivedContract(stakeholders)) =>
      ContractStateValue.Archived(stakeholders)
    case None => ContractStateValue.NotFound
  }

  private val toKeyCacheValue: KeyState => ContractKeyStateValue = {
    case LedgerDaoContractsReader.KeyAssigned(contractId, stakeholders) =>
      Assigned(contractId, stakeholders)
    case LedgerDaoContractsReader.KeyUnassigned =>
      Unassigned
  }

  private def readThroughKeyCache(
      key: GlobalKey
  )(implicit loggingContext: LoggingContext) = {
    val currentCacheSequentialId = cacheIndex.getSequentialId
    val eventualResult = contractsReader.lookupKeyState(key, currentCacheSequentialId)
    val eventualValue = eventualResult.map(toKeyCacheValue)

    for {
      _ <- keyCache.putAsync(key, currentCacheSequentialId, eventualValue)
      value <- eventualValue
    } yield value
  }

  private def nonEmptyIntersection[T](one: Set[T], other: Set[T]): Boolean =
    one.intersect(other).nonEmpty

  private def updateOffsets(event: ContractStateEvent): Unit = {
    cacheIndex.set(event.eventOffset, event.eventSequentialId)
    metrics.daml.execution.cache.indexSequentialId
      .updateValue(event.eventSequentialId)
    event match {
      case LedgerEndMarker(eventOffset, _) => signalNewLedgerHead(eventOffset)
      case _ => ()
    }
  }

  private val updateCaches: ContractStateEvent => Unit = {
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
    case _: LedgerEndMarker => ()
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
          s"State events update: Created(contractId=$contractId, globalKey=$globalKey, offset=$eventOffset, eventSequentialId=$eventSequentialId)"
        )
      case ContractStateEvent.Archived(
            contractId,
            globalKey,
            _,
            eventOffset,
            eventSequentialId,
          ) =>
        logger.debug(
          s"State events update: Archived(contractId=$contractId, globalKey=$globalKey, offset=$eventOffset, eventSequentialId=$eventSequentialId)"
        )
      case LedgerEndMarker(eventOffset, eventSequentialId) =>
        logger.debug(
          s"Ledger end reached: $eventOffset -> $eventSequentialId"
        )
    }

  override def close(): Unit = {
    contractStateUpdateKillSwitch.shutdown()

    discard(Await.ready(contractStateUpdateDone, 10.seconds))
  }
}

object MutableCacheBackedContractStore {
  type EventSequentialId = Long
  // Signal externally that the cache has caught up until the provided ledger head offset
  type SignalNewLedgerHead = Offset => Unit
  // Subscribe to the contract state events stream starting at a specific event_offset and event_sequential_id
  type SubscribeToContractStateEvents = (Offset, Long) => Source[ContractStateEvent, NotUsed]

  private[cache] def apply(
      contractsReader: LedgerDaoContractsReader,
      signalNewLedgerHead: SignalNewLedgerHead,
      subscribeToContractStateEvents: SubscribeToContractStateEvents,
      metrics: Metrics,
      maxContractsCacheSize: Long,
      maxKeyCacheSize: Long,
      minBackoffStreamRestart: FiniteDuration = 100.millis,
  )(implicit
      mat: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): MutableCacheBackedContractStore =
    new MutableCacheBackedContractStore(
      metrics,
      contractsReader,
      signalNewLedgerHead,
      subscribeToContractStateEvents,
      ContractKeyStateCache(maxKeyCacheSize, metrics),
      ContractsStateCache(maxContractsCacheSize, metrics),
      minBackoffStreamRestart,
    )

  def owner(
      contractsReader: LedgerDaoContractsReader,
      signalNewLedgerHead: Offset => Unit,
      subscribeToContractStateEvents: SubscribeToContractStateEvents,
      metrics: Metrics,
      maxContractsCacheSize: Long,
      maxKeyCacheSize: Long,
  )(implicit
      mat: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Resource[MutableCacheBackedContractStore] =
    Resource.successful(
      MutableCacheBackedContractStore(
        contractsReader,
        signalNewLedgerHead,
        subscribeToContractStateEvents,
        metrics,
        maxContractsCacheSize,
        maxKeyCacheSize,
      )
    )

  final case class ContractNotFound(contractIds: Set[ContractId])
      extends IllegalArgumentException(
        s"One or more of the following contract identifiers has been found: ${contractIds.map(_.coid).mkString(", ")}"
      )

  final case class EmptyContractIds()
      extends IllegalArgumentException(
        "Cannot lookup the maximum ledger time for an empty set of contract identifiers"
      )

  private[cache] class CacheIndex {
    private val offsetRef =
      new AtomicReference(Offset.beforeBegin -> EventSequentialId.beforeBegin)

    def set(offset: Offset, sequentialId: EventSequentialId): Unit =
      offsetRef.set(offset -> sequentialId)

    def get: (Offset, EventSequentialId) = offsetRef.get()

    def getSequentialId: EventSequentialId = offsetRef.get()._2

    def getOffset: Offset = offsetRef.get()._1
  }
}
