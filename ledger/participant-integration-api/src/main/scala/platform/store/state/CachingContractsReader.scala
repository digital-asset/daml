package com.daml.platform.store.state

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.GlobalKey
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.dao.events.ContractStateEventsReader.ContractStateEvent
import com.daml.platform.store.dao.events.ContractStateEventsReader.ContractStateEvent.LedgerEndMarker
import com.daml.platform.store.dao.events.{Contract, ContractId, ContractsReader}
import com.daml.platform.store.state.ContractsKeyCache.{Assigned, KeyStateUpdate, Unassigned}
import com.daml.platform.store.state.ContractsStateCache._

import scala.concurrent.{ExecutionContext, Future}

private[platform] class CachingContractsReader private[store] (
    metrics: Metrics,
    store: ContractsReader,
    keyCache: StateCache[GlobalKey, KeyStateUpdate],
    contractsCache: StateCache[ContractId, ContractCacheValue],
    signalGlobalNewLedgerEnd: Offset => Unit,
)(implicit
    executionContext: ExecutionContext
) extends ContractStore {
  private val headsToBeSignaled = new ConcurrentLinkedQueue[(Offset, Long, Long)]
  private val logger = ContextualizedLogger.get(getClass)
  private[store] val cacheIndex = new AtomicLong(0L)

  // Make sure the cache is up to date before completions and transactions streams
  def signalNewHead(implicit loggingContext: LoggingContext): ((Offset, Long)) => Unit = {
    case (offset, eventSequentialId) =>
      if (eventSequentialId <= cacheIndex.get()) {
        signalGlobalNewLedgerEnd(offset)
      } else {
        val enqueuedAt = System.nanoTime()
        logger.debug(s"Enqueued new head ${(offset, eventSequentialId, enqueuedAt)}")
        headsToBeSignaled.add((offset, eventSequentialId, enqueuedAt))
        ()
      }
  }

  def consumeFrom(implicit
      loggingContext: LoggingContext
  ): Flow[ContractStateEvent, Unit, NotUsed] =
    Flow[ContractStateEvent]
      .map {
        case el @ ContractStateEvent.Created(
              contractId,
              _,
              globalKey,
              _,
              eventOffset,
              eventSequentialId,
            ) =>
          logger.debug(
            s"State events update: Created(contractId=$contractId, globalKey=$globalKey, offset=$eventOffset, eventSequentialId=$eventSequentialId"
          )
          el
        case el @ ContractStateEvent.Archived(
              contractId,
              _,
              eventOffset,
              eventSequentialId,
            ) =>
          logger.debug(
            s"State events update: Archived(contractId=$contractId, offset=$eventOffset, eventSequentialId=$eventSequentialId"
          )
          el
        case el @ LedgerEndMarker(eventOffset, eventSequentialId) =>
          logger.debug(
            s"Ledger end reached: $eventOffset -> $eventSequentialId "
          )
          el
      }
      .map {
        case ContractStateEvent.Created(
              contractId,
              contract,
              globalKey,
              flatEventWitnesses,
              _,
              eventSequentialId,
            ) =>
          globalKey.foreach(
            keyCache.put(_, eventSequentialId, Assigned(contractId, flatEventWitnesses))
          )
          contractsCache.put(contractId, eventSequentialId, Active(contract, flatEventWitnesses))
          eventSequentialId
        case ContractStateEvent.Archived(
              contractId,
              stakeholders,
              _,
              eventSequentialId,
            ) =>
          contractsCache.put(
            contractId,
            eventSequentialId,
            Archived(eventSequentialId, stakeholders),
          )
          eventSequentialId
        case other => other.eventSequentialId // Just pass the seq id downstream
      }
      .map(idx => {
        cacheIndex.set(idx)
        Option(headsToBeSignaled.peek())
          .foreach { oldestHeadInQueue =>
            logger.debug(s"New cache index $idx vs oldest head in queue $oldestHeadInQueue")
            headsToBeSignaled.removeIf { case dekd @ (offset, seqId, enqueuedAt) =>
              if (seqId <= idx) {
                logger.debug(s"Dequeued $dekd and signaling new ledger end")
                metrics.daml.index.cacheCatchup
                  .update(System.nanoTime() - enqueuedAt, TimeUnit.NANOSECONDS)
                signalGlobalNewLedgerEnd(offset)
                true
              } else false
            }
            ()
          }

        metrics.daml.indexer.currentStateCacheSequentialIdGauge.updateValue(idx)
      })

  override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    keyCache.get(key) match {
      case Some(Assigned(contractId, parties)) if `intersection non-empty`(readers, parties) =>
        lookupActiveContract(readers, contractId).map(_.map(_ => contractId))
      case Some(_) => Future.successful(None)
      case None => readThroughKeyCache(key, readers)
    }

  override def lookupActiveContract(readers: Set[Party], contractId: ContractId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Contract]] =
    contractsCache
      .get(contractId)
      .map {
        case Active(contract, stakeholders) if `intersection non-empty`(stakeholders, readers) =>
          Future.successful(Some(contract))
        case Archived(_, stakeholders) if `intersection non-empty`(stakeholders, readers) =>
          Future.successful(Option.empty)
        case NotFound =>
          logger.warn(s"Contract not found for $contractId")
          Future.successful(Option.empty)
        case _: ExistingContractValue =>
          logger.debug(s"Checking divulgence for contractId=$contractId and readers=$readers")
          store.lookupActiveContractAndLoadArgument(readers, contractId)
      }
      .getOrElse {
        readThroughStateCache(contractId, readers)
      }

  private def readThroughStateCache(contractId: ContractId, readers: Set[Party])(implicit
      loggingContext: LoggingContext
  ) = {
    val currentCacheOffset = cacheIndex.get()
    val eventualResult =
      Timed.future(
        metrics.daml.index.lookupContract,
        store.lookupContract(contractId, currentCacheOffset),
      )
    contractsCache.putAsync(
      key = contractId,
      validAt = currentCacheOffset,
      eventualValue = eventualResult.collect {
        case Some(Left((contract, stakeholders, _))) =>
          ContractsStateCache.Active(contract, stakeholders)
        case Some(Right((archivedAt, stakeholders))) =>
          ContractsStateCache.Archived(archivedAt, stakeholders)
        case None => NotFound
      },
    )

    eventualResult.flatMap[Option[Contract]] {
      case Some(Left((contract, stakeholders, _)))
          if `intersection non-empty`(stakeholders, readers) =>
        Future.successful(Some(contract))
      case Some(Right((_, stakeholders))) if `intersection non-empty`(stakeholders, readers) =>
        Future.successful(Option.empty[Contract])
      case Some(_) =>
        //  Use optimized lookup only for divulgence
        store.lookupActiveContractAndLoadArgument(readers, contractId)
      case None => Future.successful(Option.empty[Contract])
    }
  }

  private def readThroughKeyCache(
      key: GlobalKey,
      readers: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[ContractId]] = {
    val currentCacheOffset = cacheIndex.get()
    val eventualResult = store.lookupContractKey(key, currentCacheOffset)

    keyCache.putAsync(
      key,
      currentCacheOffset,
      eventualResult.map {
        case Some((contractId, stakeholders)) =>
          Assigned(contractId, stakeholders)
        case None => Unassigned
      },
    )

    eventualResult.map(_.collect {
      case (id, parties) if `intersection non-empty`(readers, parties) =>
        id
    })
  }

  private def `intersection non-empty`[T](one: Set[T], other: Set[T]): Boolean =
    one.toStream.intersect(other.toStream).nonEmpty

  override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Instant]] =
    store.lookupMaximumLedgerTime(ids)
}

object CachingContractsReader {
  def apply(
      store: ContractsReader,
      metrics: Metrics,
      globallySignalNewLedgerEnd: Offset => Unit,
      stateCacheSize: Long = 100000L,
      keyCacheSize: Long = 100000L,
  )(implicit
      executionContext: ExecutionContext
  ): CachingContractsReader =
    new CachingContractsReader(
      metrics,
      store,
      ContractsKeyCache(metrics, keyCacheSize),
      ContractsStateCache(metrics, stateCacheSize),
      globallySignalNewLedgerEnd,
    )
}
