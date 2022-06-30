// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.codahale.metrics.Timer
import com.daml.caching.Cache
import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Timed
import com.daml.platform.store.cache.MutableCacheBackedContractStore.ContractReadThroughNotFound
import com.daml.platform.store.cache.StateCache.PendingUpdatesState
import com.daml.scalautil.Statement.discard

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** This class is a wrapper around a Caffeine cache designed to handle correct resolution of
  * concurrent updates for the same key.
  *
  * The [[StateCache]] tracks its own notion of logical time with the `cacheIndex`
  * which evolves monotonically based on the index DB's offset (updated by [[putBatch]]).
  *
  * The cache's logical time (i.e. the `cacheIndex`) is used for establishing precedence of cache updates
  * stemming from read-throughs triggered from command interpretation on cache misses.
  */
private[platform] case class StateCache[K, V](
    initialCacheIndex: Offset,
    cache: Cache[K, V],
    registerUpdateTimer: Timer,
)(implicit ec: ExecutionContext) {
  private val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private[cache] val pendingUpdates = mutable.Map.empty[K, PendingUpdatesState]
  @volatile private[cache] var cacheIndex = initialCacheIndex

  /** Fetch the corresponding value for an input key, if present.
    *
    * @param key the key to query for
    * @return optionally [[V]]
    */
  def get(key: K)(implicit loggingContext: LoggingContext): Option[V] =
    cache.getIfPresent(key) match {
      case Some(value) =>
        logger.debug(s"Cache hit for $key -> ${truncateValueForLogging(value)}")
        Some(value)
      case None =>
        logger.debug(s"Cache miss for $key ")
        None
    }

  /** Synchronous cache updates evolve the cache ahead with the most recent Index DB entries.
    * This method increases the `cacheIndex` monotonically.
    *
    * @param validAt ordering discriminator for pending updates for the same key
    * @param batch the batch of events updating the cache at `validAt`
    */
  def putBatch(validAt: Offset, batch: Map[K, V])(implicit loggingContext: LoggingContext): Unit =
    Timed.value(
      registerUpdateTimer, {
        pendingUpdates.synchronized {
          // The mutable contract state cache update stream should generally increase the cacheIndex strictly monotonically.
          // However, the most recent updates can be replayed in case of failure of the mutable contract state cache update stream.
          // In this case, we must ignore the already seen updates (i.e. that have `validAt` before or at the cacheIndex).
          if (validAt > cacheIndex) {
            batch.keySet.foreach { key =>
              pendingUpdates
                .get(key)
                .foreach(_.latestValidAt = validAt)
            }
            cacheIndex = validAt
            cache.putAll(batch)
            logger.debug(
              s"Updated cache with a batch of ${batch
                  .map { case (k, v) => s"$k -> ${truncateValueForLogging(v)}" }
                  .mkString("[", ", ", "]")} at $validAt"
            )
          } else
            logger.warn(
              s"Ignoring incoming synchronous update at an index ($validAt) equal to or before the cache index ($cacheIndex)"
            )
        }
      },
    )

  /** Update the cache asynchronously.
    *
    * In face of multiple in-flight updates competing for the `key`,
    * this method registers an async update to the cache
    * only if the to-be-inserted tuple is the most recent
    * (i.e. it has `validAt` highest amongst the competing updates).
    *
    * @param key the key at which to update the cache
    * @param fetchAsync fetches asynchronously the value for key `key` at the current cache index
    */
  def putAsync(key: K, fetchAsync: Offset => Future[V])(implicit
      loggingContext: LoggingContext
  ): Future[V] = Timed.value(
    registerUpdateTimer,
    pendingUpdates.synchronized {
      val validAt = cacheIndex
      val eventualValue = Future.delegate(fetchAsync(validAt))
      val pendingUpdatesForKey = pendingUpdates.getOrElseUpdate(key, PendingUpdatesState.empty)
      if (pendingUpdatesForKey.latestValidAt < validAt) {
        pendingUpdatesForKey.latestValidAt = validAt
        pendingUpdatesForKey.pendingCount += 1
        registerEventualCacheUpdate(key, eventualValue, validAt)
          .flatMap(_ => eventualValue)
      } else eventualValue
    },
  )

  /** Resets the cache and cancels are pending asynchronous updates.
    *
    * @param resetAtOffset The cache re-initialization offset
    */
  def reset(resetAtOffset: Offset): Unit =
    pendingUpdates.synchronized {
      cacheIndex = resetAtOffset
      pendingUpdates.clear()
      cache.invalidateAll()
    }

  private def registerEventualCacheUpdate(
      key: K,
      eventualUpdate: Future[V],
      validAt: Offset,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    eventualUpdate
      .map { (value: V) =>
        pendingUpdates.synchronized {
          pendingUpdates
            .get(key)
            .map { pendingForKey =>
              // Only update the cache if the current update is targeting the cacheIndex
              // sampled when initially dispatched in `putAsync`.
              // Otherwise we can assume that a more recent `putAsync` has an update in-flight
              // or that the entry has been updated synchronously with `put` with a recent Index DB entry.
              if (pendingForKey.latestValidAt == validAt) {
                cache.put(key, value)
                logger.debug(
                  s"Updated cache for $key with ${truncateValueForLogging(value)} at $validAt"
                )
              }
              removeFromPending(key)
            }
            .getOrElse(logger.error(s"Pending updates tracker for $key not registered "))
        }
      }
      .recover {
        // Negative contract lookups are forwarded to `putAsync` as failed futures as they should not be cached
        // since they can still resolve on subsequent divulgence lookups (see [[MutableCacheBackedContractStore.readThroughContractsCache]]).
        // Hence, this scenario is not considered an error condition and should not be logged as such.
        // TODO Remove this type-check when properly caching divulgence lookups
        case contractNotFound: ContractReadThroughNotFound =>
          pendingUpdates.synchronized {
            removeFromPending(key)
          }
          logger.debug(s"Not caching negative lookup for contract at key $key", contractNotFound)
        case err =>
          pendingUpdates.synchronized {
            removeFromPending(key)
          }
          logger.warn(s"Failure in pending cache update for key $key", err)
      }

  private def removeFromPending(key: K)(implicit loggingContext: LoggingContext): Unit =
    discard(
      pendingUpdates
        .get(key)
        .map { pendingForKey =>
          pendingForKey.pendingCount -= 1
          if (pendingForKey.pendingCount == 0L) {
            pendingUpdates -= key
          }
        }
        .getOrElse {
          logger.error(s"Expected pending updates tracker for key $key is missing")
        }
    )

  private def truncateValueForLogging(value: V) = {
    val stringValueRepr = value.toString
    val maxValueLength = 250
    if (stringValueRepr.length > maxValueLength)
      stringValueRepr.take(maxValueLength) + "..."
    else stringValueRepr
  }
}

object StateCache {

  /** Used to track competing updates to the cache for a specific key.
    * @param pendingCount The number of in-progress updates.
    * @param latestValidAt Highest version of any pending update.
    */
  private[cache] case class PendingUpdatesState(
      var pendingCount: Long,
      var latestValidAt: Offset,
  )
  private[cache] object PendingUpdatesState {
    def empty: PendingUpdatesState = PendingUpdatesState(
      0L,
      Offset.beforeBegin,
    )
  }
}
