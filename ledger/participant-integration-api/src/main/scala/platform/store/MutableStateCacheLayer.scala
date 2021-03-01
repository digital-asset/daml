package com.daml.platform.store

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import com.daml.caching.{Cache, SizedCache}
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.MutableStateCacheLayer.KeyCacheValue

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

class MutableStateCacheLayer(
    store: ContractStore,
    keyCache: Cache[GlobalKey, KeyCacheValue],
)(implicit
    executionContext: ExecutionContext
) extends ContractStore {
  private val currentOffset = new AtomicReference[Offset](Offset.beforeBegin)

  override def lookupActiveContract(readers: Set[Party], contractId: ContractId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Value.ContractInst[Value.VersionedValue[ContractId]]]] =
    store.lookupActiveContract(readers, contractId)

  override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    fetch(key, readers)

  override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Instant]] =
    store.lookupMaximumLedgerTime(ids)

  private def fetch(key: GlobalKey, readers: Set[Party] /* Use this */ )(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    Future
      .successful(keyCache.getIfPresent(key))
      .flatMap {
        case None =>
          readThroughCache(key).map(_.collect {
            case (id, parties) if `intersection non-empty`(readers, parties) => id
          })
        case Some(Some((contractId, parties))) if `intersection non-empty`(readers, parties) =>
          Future.successful(Some(contractId))
        case _ => Future.successful(None)
      }

  private def readThroughCache(
      key: GlobalKey
  )(implicit loggingContext: LoggingContext): Future[Option[(ContractId, Set[Party])]] = {
    val currentCacheOffset = currentOffset.get()
    store
      .lookupContractKey(key)
      .andThen { case Success((maybeLastCreated, maybeLastDeleted)) =>
        (maybeLastCreated, maybeLastDeleted) match {
          case (Some(lastCreated), Some(lastDeleted))
              if lastCreated._1 <= currentCacheOffset && lastDeleted._1 <= currentCacheOffset =>
            keyCache.put(key, Option.empty)
          case (Some((createdAt, contractId, parties)), _) if createdAt <= currentCacheOffset =>
            keyCache.put(key, Some(contractId -> parties))
//          case (None, _) => keyCache.put(key, Option.empty) // TDT Tricky one: can it cause incorrect cache state? Slim chances
          case _ => ()
        }
      }
      .map { case (lastCreate, lastArchive) =>
        lastArchive
          .map { _ => Option.empty }
          .getOrElse {
            lastCreate.map { case (_, id, parties) =>
              id -> parties
            }
          }
      }
  }

  private def `intersection non-empty`[T](one: Set[T], other: Set[T]): Boolean =
    one.toStream.intersect(other.toStream).nonEmpty

  def lookupContractKey(key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[(Option[(Offset, ContractId, Set[Party])], Option[(Offset, ContractId)])] =
    Future.successful(Option.empty -> Option.empty)
}

object MutableStateCacheLayer {
  type KeyCacheValue = Option[(ContractId, Set[Party])]

  def apply(store: ContractStore, metrics: Metrics)(implicit
      executionContext: ExecutionContext
  ): MutableStateCacheLayer =
    new MutableStateCacheLayer(
      store,
      SizedCache.from(SizedCache.Configuration(10000L), metrics.daml.execution.keyStateCache),
    )(executionContext)
}
