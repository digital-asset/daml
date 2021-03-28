package com.daml.platform.store.cache

import java.util.concurrent.atomic.AtomicLong

import com.daml.caching.Cache
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.StateCache.{ConcurrentUpdateError, PendingUpdates}

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}

/** This class is a wrapper around a Caffeine cache designed to handle
  * correct resolution of concurrent updates for the same key.
  */
private[platform] case class StateCache[K, V](cache: Cache[K, V], metrics: Metrics)(implicit
    ec: ExecutionContext
) {
  private val maximumAsyncPutRetryLimit = 10
  private val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  private[store] val pendingUpdates = TrieMap.empty[K, PendingUpdates]

  /** Fetch the corresponding value for an input key, if present.
    *
    * @param key the key to query for
    * @return optionally [[V]]
    */
  def get(key: K)(implicit loggingContext: LoggingContext): Option[V] =
    cache.getIfPresent(key) match {
      case Some(value) =>
        logger.debug(s"Cache hit for $key -> ${value.toString.take(100)}")
        Some(value)
      case None =>
        logger.debug(s"Cache miss for $key ")
        None
    }

  /** Update the cache synchronously.
    *
    * In face of multiple in-flight updates competing for the `key` (see [[putAsync()]]),
    * this method updates the cache only if the to-be-inserted tuple is the most recent
    * (i.e. it has `validAt` highest amongst the competing updates).
    *
    * @param key the key at which to update the cache
    * @param validAt ordering discriminator for pending updates for the same key
    * @param value the value to insert
    */
  def put(key: K, validAt: Long, value: V): Unit = {
    val competingLatestForKey =
      pendingUpdates
        .get(key)
        .map { pending =>
          pending.latestValidAt.getAndUpdate { latestUpdate =>
            if (latestUpdate <= validAt) validAt else latestUpdate
          }
        }
        .getOrElse(Long.MinValue)
    if (competingLatestForKey <= validAt) cache.put(key, value) else ()
  }

  /** Update the cache asynchronously.
    *
    * This method ensures insertion order in face of concurrent updates for the same `key`.
    *
    * @param key the key at which to update the cache
    * @param validAt ordering discriminator for pending updates for the same key
    * @param eventualValue the eventual result
    */
  final def putAsync(key: K, validAt: Long, eventualValue: Future[V])(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    putAsyncWithRecursionLimit(key, validAt, eventualValue, retryCount = 0)

  @tailrec
  private final def putAsyncWithRecursionLimit(
      key: K,
      validAt: Long,
      eventualValue: Future[V],
      retryCount: Int,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Unit] = {
    logger.debug(s"New pending cache update for key $key at $validAt")
    val pendingUpdate @ PendingUpdates(pendingCountRef, latestRef) =
      pendingUpdates.getOrElseUpdate(key, PendingUpdates.empty)
    /* We need to synchronize here instead of using a lock-free update
     * since registering the next update with `registerEventualCacheUpdate` is not idempotent
     * and cannot be retried/cancelled on thread contention.
     */
    val promise = Promise[Unit]
    val needsRetry = pendingUpdate.synchronized {
      if (latestRef.get() >= validAt) {
        /* Invalidated by a more recent put or a more recent pending update */
        promise.completeWith(Future.unit)
        false
      } else if (latestRef.get() == Long.MinValue) {
        /* There no other concurrent updates for this key */
        pendingCountRef.set(1L)
        latestRef.set(validAt)
        promise.completeWith(registerEventualCacheUpdate(key, eventualValue, validAt))
        false
      } else if (latestRef.get() > Long.MinValue && pendingCountRef.get() > 0L) {
        /* There are other concurrent updates for the key */
        if (pendingCountRef.getAndIncrement() > 0L) {
          /* Check again that the pending update reference has not been removed */
          latestRef.set(validAt)
          promise.completeWith(registerEventualCacheUpdate(key, eventualValue, validAt))
          false
        } else {
          /* If the pending update reference has been removed, go for a retry if the current update is the latest */
          latestRef.get() >= validAt
        }
      } else {
        /* We need to retry since another update has raced through */
        true
      }
    }

    if (!needsRetry) {
      promise.future
    } else if (needsRetry && maximumAsyncPutRetryLimit > retryCount) {
      logger.warn(s"Race condition when trying to update cache for $key at $validAt. Retrying..")
      metrics.daml.execution.mutableStateCacheUpdateRetries.inc()
      putAsyncWithRecursionLimit(key, validAt, eventualValue, retryCount + 1)
    } else
      Future.failed(
        ConcurrentUpdateError(s"Failed updating the cache for key $key after $retryCount retries.")
      )
  }

  private def registerEventualCacheUpdate(
      key: K,
      eventualUpdate: Future[V],
      validAt: Long,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    eventualUpdate.transform[Unit](
      (value: V) => {
        // Check again that the update is the latest
        if (pendingUpdates(key).latestValidAt.get() == validAt) {
          logger.debug(s"Updating cache with $key -> ${value.toString.take(100)}")
          cache.put(key, value)
        }
        removeFromPending(key)
      },
      (err: Throwable) => {
        removeFromPending(key)
        logger.warn(s"Failure in pending cache update for key $key", err)
        err
      },
    )

  private def removeFromPending(key: K): Unit =
    if (pendingUpdates(key).pendingCount.updateAndGet(_ - 1) == 0L) {
      pendingUpdates -= key
    }
}

object StateCache {

  /** Used to track competing updates to the cache for a specific key.
    * @param pendingCount The number of in-progress updates.
    * @param latestValidAt Highest version of any pending update.
    */
  case class PendingUpdates(
      pendingCount: AtomicLong,
      latestValidAt: AtomicLong,
  )
  object PendingUpdates {
    def empty: PendingUpdates = PendingUpdates(
      new AtomicLong(0L),
      new AtomicLong(Long.MinValue),
    )
  }

  case class ConcurrentUpdateError(msg: String) extends RuntimeException(msg)
}
