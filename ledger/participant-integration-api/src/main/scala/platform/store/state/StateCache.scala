package com.daml.platform.store.state

import java.util.concurrent.atomic.AtomicReference

import com.daml.caching.Cache
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.state.StateCache.PendingUpdates

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait StateCache[K, V] {
  protected implicit def ec: ExecutionContext
  protected def cache: Cache[K, V]

  protected val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  private[store] val pendingUpdates =
    collection.concurrent.TrieMap.empty[K, AtomicReference[PendingUpdates]]

  def fetch(key: K)(implicit loggingContext: LoggingContext): Option[V] =
    cache.getIfPresent(key) match {
      case Some(value) =>
        logger.debug(s"Cache hit for $key -> $value")
        Some(value)
      case None =>
        logger.debug(s"Cache miss for $key ")
        None
    }

  def feedAsync(key: K, validAt: Long, newUpdate: Future[V]): Future[Unit] = {
    pendingUpdates
      .getOrElseUpdate(key, new AtomicReference(PendingUpdates.empty))
      .updateAndGet { case current @ PendingUpdates(pendingCount, highestIndex, effectsChain) =>
        if (highestIndex >= validAt) current
        else
          PendingUpdates(
            pendingCount = pendingCount + 1,
            highestIndex = validAt,
            effectsChain = effectsChain.transformWith { _ =>
              registerEventualCacheUpdate(validAt, key, newUpdate).map(_ => ())
            },
          )
      }
      .effectsChain
  }

  private def registerEventualCacheUpdate(
      validAt: Long,
      key: K,
      eventualUpdate: Future[V],
  ): Future[V] =
    eventualUpdate.andThen {
      case Success(update) =>
        // Double-check if we need to update
        if (pendingUpdates(key).get().highestIndex == validAt) {
          cache.put(key, update)
        }
        removeFromPending(key)
      case Failure(_) => removeFromPending(key)
    }

  private def removeFromPending(key: K): Unit =
    if (
      pendingUpdates(key).updateAndGet { pending =>
        pending.copy(pendingCount = pending.pendingCount - 1)
      }.pendingCount == 0L
    ) pendingUpdates -= key
}

object StateCache {
  case class PendingUpdates(pendingCount: Long, highestIndex: Long, effectsChain: Future[Unit])
  object PendingUpdates {
    val empty: PendingUpdates = PendingUpdates(0L, Long.MinValue, Future.unit)
  }
}
