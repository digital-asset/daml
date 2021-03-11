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
        logger.debug(s"Cache hit for $key -> ${value.toString.take(100)}")
        Some(value)
      case None =>
        logger.debug(s"Cache miss for $key ")
        None
    }

  def feedAsync(key: K, validAt: Long, newUpdate: Future[V])(implicit
      loggingContext: LoggingContext
  ): Unit = {
    logger.debug(s"New pending cache update for key $key at $validAt")
    val pendingUpdate @ PendingUpdates(pendingCountRef, latestRef, effectsChain) =
      pendingUpdates.getOrElseUpdate(key, PendingUpdates.empty)

    /* We need to synchronize here instead of using a lock-free update
     * since enqueueing the next update in the chain is impure
     * and cannot be retried/cancelled on thread contention.
     */
    val needsRetry = pendingUpdate.synchronized {
      /* Invalidated by a more recent put or a more recent pending update */
      if (latestRef.get() >= validAt) false
      /* Is this the first update in this effect chain? */
      else if (latestRef.get() == Long.MinValue) {
        pendingCountRef.set(1L)
        latestRef.set(validAt)
        effectsChain
          .updateAndGet(_.transformWith { _ =>
            registerEventualCacheUpdate(key, newUpdate, validAt).map(_ => ())
          })
        false
      } else if (othersPending(pendingCountRef, latestRef)) {
        /* If 0 is returned it means another update is going to delete this entry,
         * we need to retry.
         */
        if (pendingCountRef.getAndIncrement() > 0L) {
          latestRef.set(validAt)
          effectsChain.updateAndGet(_.transformWith { _ =>
            registerEventualCacheUpdate(key, newUpdate, validAt).map(_ => ())
          })
          false
        } else latestRef.get() >= validAt
      } else true
    }
    if (needsRetry) {
      logger.warn(s"Race condition when trying to update cache for $key at $validAt. Retrying..")
      feedAsync(key, validAt, newUpdate)
    } else ()
  }

  private def othersPending(pendingCount: AtomicLong, highestIndex: AtomicLong) =
    highestIndex.get() >= 0L && pendingCount.get() > 0L

  private def registerEventualCacheUpdate(
      key: K,
      eventualUpdate: Future[V],
      validAt: Long,
  )(implicit loggingContext: LoggingContext): Future[V] =
    eventualUpdate.andThen {
      case Success(update) =>
        // Double-check if we need to update
        if (pendingUpdates(key).latestRef.get() == validAt) {
          logger.debug(s"Updating cache with $key -> ${update.toString.take(100)}")
          cache.put(key, update)
        }
        removeFromPending(key)
      case Failure(e) =>
        removeFromPending(key)
        logger.warn(s"Failure in pending cache update for key $key", e)
    }

  private def removeFromPending(key: K): Unit =
    if (pendingUpdates(key).pendingCount.updateAndGet(_ - 1) == 0L) pendingUpdates -= key
}

object StateCache {
  case class PendingUpdates(
      pendingCount: AtomicLong,
      latestRef: AtomicLong,
      effectsChain: AtomicReference[Future[Unit]],
  )
  object PendingUpdates {
    def empty: PendingUpdates = PendingUpdates(
      new AtomicLong(0L),
      new AtomicLong(Long.MinValue),
      new AtomicReference(Future.unit),
    )
  }
}
