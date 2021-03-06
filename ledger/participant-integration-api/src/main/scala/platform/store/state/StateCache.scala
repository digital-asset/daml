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

  def feedAsync(key: K, validAt: Long, newUpdate: Future[V])(implicit
      loggingContext: LoggingContext
  ): Future[Unit] = {
    logger.debug(s"New pending cache update for key $key at $validAt")
    val pendingUpdate @ PendingUpdates(pendingCount, highestIndex, effectsChain) = pendingUpdates
      .getOrElseUpdate(key, PendingUpdates.empty)
    // We need to synchronize here instead of using a lock-free update
    // since registering the next update is impure and cannot be retried/discarded
    // on thread contention.
    pendingUpdate.synchronized {
      if (highestIndex.get() >= validAt) {
        logger.trace(
          s"Not registering update for key $key. Highest index ${highestIndex.get()} vs $validAt. Pending updates map $pendingUpdates"
        )
        Future.unit
      } else {
        logger.trace(
          s"Registered pending update for key $key. Highest index ${highestIndex.get()} and $validAt. Pending updates map $pendingUpdates"
        )
        highestIndex.set(validAt)
        pendingCount.incrementAndGet()
        effectsChain.updateAndGet(_.transformWith { _ =>
          registerEventualCacheUpdate(key, newUpdate).map(_ => ())
        })
      }
    }
  }

  private def registerEventualCacheUpdate(
      key: K,
      eventualUpdate: Future[V],
  )(implicit loggingContext: LoggingContext): Future[V] =
    eventualUpdate.andThen {
      case Success(update) =>
        // Double-check if we need to update
//        if (pendingUpdates(key).highestIndex.get() == validAt) {
        logger.debug(s"Updating cache with $key -> $update ")
        cache.put(key, update)
        removeFromPending(key)
      case Failure(e) =>
        removeFromPending(key)
        logger.warn(s"Failure in pending cache update for key $key", e)
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
    def empty: PendingUpdates = PendingUpdates(
      new AtomicLong(0L),
      new AtomicLong(Long.MinValue),
      new AtomicReference(Future.unit),
    )
  }
}
