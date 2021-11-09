// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.stream.{KillSwitches, Materializer, RestartSettings, UniqueKillSwitch}
import akka.{Done, NotUsed}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.transaction.GlobalKey
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
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

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import com.daml.platform.apiserver.execution.MissingContracts

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace
import scala.util.{Success, Try}

private[platform] class MutableCacheBackedContractStore(
    metrics: Metrics,
    contractsReader: LedgerDaoContractsReader,
    signalNewLedgerHead: SignalNewLedgerHead,
    startIndexExclusive: (Offset, Long),
    private[cache] val keyCache: StateCache[GlobalKey, ContractKeyStateValue],
    private[cache] val contractsCache: StateCache[ContractId, ContractStateValue],
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends ContractStore {

  private val logger = ContextualizedLogger.get(getClass)

  private[cache] val cacheIndex = new CacheIndex(startIndexExclusive)

  def push(event: ContractStateEvent): Unit = {
    debugEvents(event)
    updateCaches(event)
    updateOffsets(event)
  }

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
    Future
      .fromTry(partitionCached(ids))
      .flatMap {
        case (cached, toBeFetched) if toBeFetched.isEmpty =>
          Future.successful(Some(cached.max))
        case (cached, toBeFetched) =>
          contractsReader
            .lookupMaximumLedgerTime(toBeFetched)
            .map(_.map(m => (cached + m).max))
      }

  private def partitionCached(
      ids: Set[ContractId]
  )(implicit
      loggingContext: LoggingContext
  ): Try[(Set[Instant], Set[ContractId])] = {
    val cacheQueried = ids.map(id => id -> contractsCache.get(id))

    val cached = cacheQueried.view
      .foldLeft[Either[Set[ContractId], Set[Instant]]](Right(Set.empty[Instant])) {
        // successful lookups
        case (Right(timestamps), (_, Some(active: Active))) =>
          Right(timestamps + active.createLedgerEffectiveTime)
        case (Right(timestamps), (_, None)) => Right(timestamps)

        // failure cases
        case (acc, (cid, Some(Archived(_) | NotFound))) =>
          val missingContracts = acc.left.getOrElse(Set.empty) + cid
          Left(missingContracts)
        case (acc @ Left(_), _) => acc
      }

    cached
      .map { cached =>
        val missing = cacheQueried.collect { case (id, None) => id }
        (cached, missing)
      }
      .left
      .map(MissingContracts)
      .toTry
  }

  private def readThroughContractsCache(contractId: ContractId)(implicit
      loggingContext: LoggingContext
  ) = {
    val currentCacheSequentialId = cacheIndex.getEventSequentialId
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
        eventualValue = eventualValue.transformWith {
          case Success(NotFound) =>
            metrics.daml.execution.cache.readThroughNotFound.inc()
            // We must not cache negative lookups by contract-id, as they can be invalidated by later divulgence events.
            // This is OK from a performance perspective, as we do not expect uses-cases that require
            // caching of contract absence or the results of looking up divulged contracts.
            Future.failed(ContractReadThroughNotFound(contractId))
          case result => Future.fromTry(result)
        },
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
      case contractStateValue =>
        // This flow is exercised when the readers are not stakeholders of the contract
        // (the contract might have been divulged to the readers)
        // OR the contract was not found in the index
        //
        logger.debug(s"Checking divulgence for contractId=$contractId and readers=$readers")
        resolveDivulgenceLookup(contractStateValue, contractId, readers)
    }

  private def resolveDivulgenceLookup(
      contractStateValue: ContractStateValue,
      contractId: ContractId,
      forParties: Set[Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[Contract]] =
    contractStateValue match {
      case Active(contract, _, _) =>
        metrics.daml.execution.cache.resolveDivulgenceLookup.inc()
        contractsReader.lookupActiveContractWithCachedArgument(
          forParties,
          contractId,
          contract.arg,
        )
      case _: Archived | NotFound =>
        // We need to fetch the contract here since the contract creation or archival
        // may have not been divulged to the readers
        metrics.daml.execution.cache.resolveFullLookup.inc()
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
    val currentCacheSequentialId = cacheIndex.getEventSequentialId
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
}

private[platform] object MutableCacheBackedContractStore {
  type EventSequentialId = Long
  // Signal externally that the cache has caught up until the provided ledger head offset
  type SignalNewLedgerHead = Offset => Unit
  // Subscribe to the contract state events stream starting at a specific event_offset and event_sequential_id
  type SubscribeToContractStateEvents =
    ((Offset, EventSequentialId)) => Source[ContractStateEvent, NotUsed]

  def apply(
      contractsReader: LedgerDaoContractsReader,
      signalNewLedgerHead: SignalNewLedgerHead,
      startIndexExclusive: (Offset, Long),
      metrics: Metrics,
      maxContractsCacheSize: Long,
      maxKeyCacheSize: Long,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): MutableCacheBackedContractStore =
    new MutableCacheBackedContractStore(
      metrics,
      contractsReader,
      signalNewLedgerHead,
      startIndexExclusive,
      ContractKeyStateCache(maxKeyCacheSize, metrics),
      ContractsStateCache(maxContractsCacheSize, metrics),
    )

  final class OwnerWithSubscription(
      subscribeToContractStateEvents: SubscribeToContractStateEvents,
      contractsReader: LedgerDaoContractsReader,
      signalNewLedgerHead: SignalNewLedgerHead,
      startIndexExclusive: (Offset, Long),
      metrics: Metrics,
      maxContractsCacheSize: Long,
      maxKeyCacheSize: Long,
      executionContext: ExecutionContext,
      minBackoffStreamRestart: FiniteDuration = 100.millis,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ) extends ResourceOwner[MutableCacheBackedContractStore] {

    private val contractStore = MutableCacheBackedContractStore(
      contractsReader,
      signalNewLedgerHead,
      startIndexExclusive,
      metrics,
      maxContractsCacheSize,
      maxKeyCacheSize,
    )(executionContext, loggingContext)

    override def acquire()(implicit
        context: ResourceContext
    ): Resource[MutableCacheBackedContractStore] =
      Resource(Future {
        RestartSource
          .withBackoff(
            RestartSettings(
              minBackoff = minBackoffStreamRestart,
              maxBackoff = 10.seconds,
              randomFactor = 0.2,
            )
          )(() =>
            subscribeToContractStateEvents(contractStore.cacheIndex.get)
              .map(contractStore.push)
          )
          .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
          .toMat(Sink.ignore)(Keep.both[UniqueKillSwitch, Future[Done]])
          .run()
      }(context.executionContext)) {
        case (contractStateUpdateKillSwitch, contractStateUpdateDone) =>
          contractStateUpdateKillSwitch.shutdown()

          contractStateUpdateDone.map(_ => ())(context.executionContext)
      }.map(_ => contractStore)
  }

  final case class ContractReadThroughNotFound(contractId: ContractId) extends NoStackTrace {
    override def getMessage: String =
      s"Contract not found for contract id $contractId. Hint: this could be due racing with a concurrent archival."
  }

  private[cache] class CacheIndex(initValue: (Offset, Long)) {
    private val offsetRef: AtomicReference[(Offset, EventSequentialId)] =
      new AtomicReference(initValue)

    def set(offset: Offset, sequentialId: EventSequentialId): Unit =
      offsetRef.set(offset -> sequentialId)

    def get: (Offset, EventSequentialId) = offsetRef.get()

    def getEventSequentialId: EventSequentialId = get._2
  }
}
