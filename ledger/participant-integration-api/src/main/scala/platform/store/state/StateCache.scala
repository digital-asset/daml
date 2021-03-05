package com.daml.platform.store.state

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

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
    collection.concurrent.TrieMap.empty[K, PendingUpdates]

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
    val pendingUpdate = pendingUpdates
      .getOrElseUpdate(key, PendingUpdates.empty)
    // We need to synchronize here instead of using a lock-free update
    // since registering the next update is impure and cannot be retried/discarded
    // on thread contention.
    pendingUpdate.synchronized {
      if (pendingUpdate.highestIndex.get() >= validAt)
        Future.unit
      else {
        pendingUpdate.highestIndex.set(validAt)
        pendingUpdate.pendingCount.incrementAndGet()
        pendingUpdate.effectsChain.updateAndGet(_.transformWith { _ =>
          registerEventualCacheUpdate(validAt, key, newUpdate).map(_ => ())
        })
      }
    }
  }

  private def registerEventualCacheUpdate(
      validAt: Long,
      key: K,
      eventualUpdate: Future[V],
  ): Future[V] =
    eventualUpdate.andThen {
      case Success(update) =>
        // Double-check if we need to update
        if (pendingUpdates(key).highestIndex.get() == validAt) {
          cache.put(key, update)
        }
        removeFromPending(key)
      case Failure(_) => removeFromPending(key)
    }

  private def removeFromPending(key: K): Unit = {
    if (pendingUpdates(key).pendingCount.updateAndGet(_ - 1) == 0L) pendingUpdates -= key
  }
}

object StateCache {
  case class PendingUpdates(
      pendingCount: AtomicLong,
      highestIndex: AtomicLong,
      effectsChain: AtomicReference[Future[Unit]],
  )
  object PendingUpdates {
    val empty: PendingUpdates = PendingUpdates(
      new AtomicLong(0L),
      new AtomicLong(Long.MinValue),
      new AtomicReference(Future.unit),
    )
  }
}
