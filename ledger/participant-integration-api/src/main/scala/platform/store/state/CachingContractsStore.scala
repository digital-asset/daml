package com.daml.platform.store.state

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.GlobalKey
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.dao.events.ContractLifecycleEventsReader.ContractStateEvent
import com.daml.platform.store.dao.events.{Contract, ContractId, ContractsReader}
import com.daml.platform.store.state.ContractsKeyCache.{
  Assigned,
  KeyCacheValue,
  KeyStateUpdate,
  Unassigned,
}
import com.daml.platform.store.state.ContractsStateCache._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

private[store] class CachingContractsStore private (
    metrics: Metrics,
    store: ContractsReader,
    keyCache: StateCache[GlobalKey, KeyStateUpdate, KeyCacheValue],
    contractsCache: StateCache[ContractId, ContractCacheValue, ContractCacheValue],
)(implicit
    executionContext: ExecutionContext,
    materializer: Materializer,
) extends ContractStore {
  private val logger = ContextualizedLogger.get(getClass)
  private val cacheIndex = new AtomicLong(0L)

  def consumeFrom(contractEvents: Source[ContractStateEvent, NotUsed]): Unit =
    contractEvents
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
            keyCache.feedAsync(
              _,
              eventSequentialId,
              Future.successful(Assigned(contractId, flatEventWitnesses)),
            )
          )
          contractsCache.feedAsync(
            contractId,
            eventSequentialId,
            Future.successful(Active(contract, flatEventWitnesses)),
          )
          eventSequentialId
        case ContractStateEvent.Archived(
              contractId,
              contract,
              globalKey,
              stakeholders,
              _,
              _,
              eventSequentialId,
            ) =>
          globalKey.foreach(
            keyCache.feedAsync(
              _,
              eventSequentialId,
              Future.successful(Unassigned),
            )
          )
          contractsCache.feedAsync(
            contractId,
            eventSequentialId,
            Future.successful(Archived(contract, stakeholders)),
          )
          eventSequentialId
      }
      .runForeach(idx => {
        cacheIndex.set(idx)
        metrics.daml.indexer.currentStateCacheSequentialIdGauge.updateValue(idx)
      })
      .foreach { _ => println("Streaming cache updater finished") } // TDT remove println

  override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    Future
      .successful(keyCache.fetch(key))
      .andThen { case Success(Some(cacheHit)) =>
        println(s"Cache hit at ${cacheIndex.get()}: $key -> $cacheHit")
      } // TDT remove println
      .flatMap {
        case None =>
          readThroughKeyCache(key).map(_.collect {
            case (id, parties) if `intersection non-empty`(readers, parties) => id
          })
        case Some(Some((contractId, parties))) if `intersection non-empty`(readers, parties) =>
          Future.successful(Some(contractId))
        case _ => Future.successful(None)
      }

  private def readThroughKeyCache(
      key: GlobalKey
  )(implicit loggingContext: LoggingContext): Future[Option[(ContractId, Set[Party])]] = {
    // Get here so we can close over this immutable `state in time` in the future below.
    val currentCacheOffset = cacheIndex.get()
    val eventualResult = store.lookupContractKey(key)

    keyCache.feedAsync(
      key,
      currentCacheOffset,
      eventualResult.map {
        case Some((createdAt, _, _, Some(archivedAt)))
            if archivedAt < currentCacheOffset && createdAt < currentCacheOffset =>
          Unassigned
        case Some((createdAt, contractId, parties, _)) if createdAt < currentCacheOffset =>
          Assigned(contractId, parties)
        case None =>
          // Safe to update since there cannot be any pending updates
          // at an earlier stage conflicting with this one  (e.g. the key is never assigned previous to this cache offset).
          Unassigned
      },
    )

    eventualResult.map {
      case Some((_, _, _, Some(_))) =>
        // Archived
        Option.empty[(ContractId, Set[Party])]
      case Some((_, contractId, parties, None)) => Some(contractId -> parties)
      case None => Option.empty[(ContractId, Set[Party])]
    }
  }

  private def `intersection non-empty`[T](one: Set[T], other: Set[T]): Boolean =
    one.toStream.intersect(other.toStream).nonEmpty

  override def lookupActiveContract(readers: Set[Party], contractId: ContractId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Contract]] =
    contractsCache
      .fetch(contractId)
      .map {
        case Active(contract, stakeholders) if `intersection non-empty`(stakeholders, readers) =>
          Future.successful(Some(contract))
        case Archived(_, stakeholders) if `intersection non-empty`(stakeholders, readers) =>
          Future.successful(Option.empty)
        case NotFound =>
          logger.warn(s"Contract not found for $contractId")
          Future.successful(Option.empty)
        case state: ExistingContractValue =>
          store.checkDivulgenceVisibility(contractId, readers).map {
            case true => Option.empty
            case false => Some(state.contract)
          }
      }
      .getOrElse {
        val currentCacheOffset = cacheIndex.get()
        val eventualResult = store.lookupContract(contractId)
        contractsCache.feedAsync(
          key = contractId,
          validAt = currentCacheOffset,
          newUpdate = eventualResult.map {
            case Some((contract, stakeholders, createdAt, Some(archivedAt)))
                if createdAt < currentCacheOffset && archivedAt < currentCacheOffset =>
              ContractsStateCache.Archived(contract, stakeholders)
            case Some((contract, stakeholders, createdAt, _)) if createdAt < currentCacheOffset =>
              ContractsStateCache.Active(contract, stakeholders)
            case None => ContractsStateCache.NotFound
          },
        )

        eventualResult.flatMap[Option[Contract]] {
          case Some((_, stakeholders, _, Some(_)))
              if `intersection non-empty`(stakeholders, readers) =>
            Future.successful(Option.empty[Contract])
          case Some((contract, stakeholders, _, None))
              if `intersection non-empty`(stakeholders, readers) =>
            Future.successful(Some(contract))
          case Some((contract, _, _, _)) =>
            store.checkDivulgenceVisibility(contractId, readers).map {
              case true => Option.empty[Contract]
              case false => Some(contract)
            }
          case None => Future.successful(Option.empty[Contract])
        }
      }

  override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Instant]] =
    store.lookupMaximumLedgerTime(ids)

  override def lookupContractKey(key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Long, ContractId, Set[Party], Option[Long])]] =
    Future.failed(new RuntimeException("should not go through here"))
}

object CachingContractsStore {
  def apply(store: ContractsReader, metrics: Metrics)(implicit
      executionContext: ExecutionContext,
      mat: Materializer,
  ): CachingContractsStore =
    new CachingContractsStore(
      metrics,
      store,
      ContractsKeyCache(metrics),
      ContractsStateCache(metrics),
    )
}
