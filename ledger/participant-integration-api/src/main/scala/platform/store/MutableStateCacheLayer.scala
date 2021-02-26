package com.daml.platform.store

import java.util.concurrent.atomic.AtomicReference

import com.daml.caching.Cache
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.MutableStateCacheLayer.KeyCacheValue

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

trait ContractStore {
  def lookupContractKey(key: GlobalKey, atOffset: Offset): Future[Option[KeyCacheValue]]
}

class MutableStateCacheLayer(
    store: ContractStore,
    metrics: Metrics,
    keyCache: Cache[GlobalKey, Option[KeyCacheValue]],
)(implicit
    loggingContext: LoggingContext,
    executionContext: ExecutionContext,
) {
  private val offset = new AtomicReference[Offset](Offset.beforeBegin)
  private val pendingUpdates = scala.collection.concurrent.TrieMap.empty[GlobalKey, Offset]

  def feed(newOffset: Offset, key: GlobalKey, value: Option[KeyCacheValue]): Unit = {
    keyCache.put(key, value)
    offset.set(newOffset)
  }

  def fetch(key: GlobalKey, readers: Set[Party] /* Use this */ ): Future[Option[ContractId]] =
    Future
      .successful(keyCache.getIfPresent(key))
      .flatMap {
        case None => readThroughCache(key)
        case Some(Some((contractId, parties))) if `intersection non-empty`(readers, parties) =>
          Future.successful(Some(contractId))
        case _ => Future.successful(None)
      }

  private def readThroughCache(key: GlobalKey): Future[Option[ContractId]] = {
    val currentCacheOffset = offset.get()
    val result = store.lookupContractKey(key, currentCacheOffset)
    registerEventualCacheUpdate(currentCacheOffset, key, result)
    result.map(_.map(_._1))
  }

  private def registerEventualCacheUpdate(
      fetchedOffset: Offset,
      key: GlobalKey,
      eventualResult: Future[Option[KeyCacheValue]],
  ): Unit = if (!pendingUpdates.get(key).exists(_ > fetchedOffset)) {
    pendingUpdates += key -> fetchedOffset
    eventualResult.andThen {
      case Success(value) =>
        pendingUpdates -= key
        keyCache.put(key, value)
      case _ => pendingUpdates -= key
    }
  }

  private def `intersection non-empty`[T](one: Set[T], other: Set[T]): Boolean =
    one.toStream.intersect(other.toStream).nonEmpty
}

object MutableStateCacheLayer {
  type KeyCacheValue = (ContractId, Set[Party])
}
