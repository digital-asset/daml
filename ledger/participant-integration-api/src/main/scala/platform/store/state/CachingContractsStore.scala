package com.daml.platform.store.state

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.GlobalKey
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.dao.events.ContractLifecycleEventsReader.ContractStateEvent
import com.daml.platform.store.dao.events.{Contract, ContractId, ContractsReader}
import com.daml.platform.store.state.ContractsKeyCache.{Assigned, KeyStateUpdate, Unassigned}
import com.daml.platform.store.state.ContractsStateCache._

import scala.concurrent.{ExecutionContext, Future}

private[store] class CachingContractsStore private[store] (
    metrics: Metrics,
    store: ContractsReader,
    keyCache: StateCache[GlobalKey, KeyStateUpdate],
    contractsCache: StateCache[ContractId, ContractCacheValue],
)(implicit
    executionContext: ExecutionContext
) extends ContractStore {
  private val logger = ContextualizedLogger.get(getClass)
  private[store] val cacheIndex = new AtomicLong(0L)

  def consumeFrom(implicit
      loggingContext: LoggingContext
  ): Flow[ContractStateEvent, Unit, NotUsed] =
    Flow[ContractStateEvent]
      .map { el =>
        println(s"Contract state events update: ${el}")
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
          contractsCache.feedAsync(
            contractId,
            eventSequentialId,
            Future.successful(Active(contract, flatEventWitnesses)),
          )
          globalKey.foreach(
            keyCache.feedAsync(
              _,
              eventSequentialId,
              Future.successful(Assigned(contractId, flatEventWitnesses)),
            )
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
      .map(idx => {
        cacheIndex.set(idx)
        logger.debug(s"New cache index $idx")
        metrics.daml.indexer.currentStateCacheSequentialIdGauge.updateValue(idx)
      })

  override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    keyCache.fetch(key) match {
      case Some(Assigned(contractId, parties)) if `intersection non-empty`(readers, parties) =>
        Future.successful(Some(contractId))
      case Some(_) =>
        Future.successful(None)
      case None =>
        readThroughKeyCache(key).map(_.collect {
          case (id, parties) if `intersection non-empty`(readers, parties) =>
            id
        })
    }

  private def readThroughKeyCache(
      key: GlobalKey
  )(implicit loggingContext: LoggingContext): Future[Option[(ContractId, Set[Party])]] = {
    val currentCacheOffset = cacheIndex.get()
    val eventualResult = store.lookupContractKey(key, currentCacheOffset)

    keyCache.feedAsync(
      key,
      currentCacheOffset,
      eventualResult.map {
        case Some((contractId, stakeholders)) => Assigned(contractId, stakeholders)
        case None => Unassigned
      },
    )

    eventualResult
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
        val eventualResult = store.lookupContract(contractId, currentCacheOffset)
        contractsCache.feedAsync(
          key = contractId,
          validAt = currentCacheOffset,
          newUpdate = eventualResult.map {
            case Some((contract, stakeholders, _, Some(_))) =>
              ContractsStateCache.Archived(contract, stakeholders)
            case Some((contract, stakeholders, _, _)) =>
              ContractsStateCache.Active(contract, stakeholders)
            case None => ContractsStateCache.NotFound
          },
        )

        eventualResult.flatMap[Option[Contract]] {
          case Some((contract, stakeholders, _, maybeArchival))
              if `intersection non-empty`(stakeholders, readers) =>
            Future.successful(
              if (maybeArchival.nonEmpty) Option.empty[Contract]
              else Some(contract)
            )
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
}

object CachingContractsStore {
  def apply(store: ContractsReader, metrics: Metrics)(implicit
      executionContext: ExecutionContext
  ): CachingContractsStore =
    new CachingContractsStore(
      metrics,
      store,
      ContractsKeyCache(metrics),
      ContractsStateCache(metrics),
    )
}
