// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.caching.Cache
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.StateCache.PendingUpdates
import com.daml.scalautil.Statement.discard

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** This class is a wrapper around a Caffeine cache designed to handle
  * correct resolution of concurrent updates for the same key.
  */
private[platform] case class StateCache[K, V](cache: Cache[K, V], metrics: Metrics)(implicit
    ec: ExecutionContext
) {
  private val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private[store] val pendingUpdates = mutable.Map.empty[K, PendingUpdates]

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
  def put(key: K, validAt: Long, value: V): Unit = pendingUpdates.synchronized {
    val competingLatestForKey =
      pendingUpdates
        .get(key)
        .map { pendingUpdate =>
          val oldLatestValidAt = pendingUpdate.latestValidAt
          pendingUpdate.latestValidAt =
            if (pendingUpdate.latestValidAt < validAt) validAt else pendingUpdate.latestValidAt
          oldLatestValidAt
        }
        .getOrElse(Long.MinValue)

    if (competingLatestForKey < validAt) cache.put(key, value) else ()
  }

  /** Update the cache asynchronously.
    *
    * In face of multiple in-flight updates competing for the `key`,
    * this method registers an async update to the cache
    * only if the to-be-inserted tuple is the most recent
    * (i.e. it has `validAt` highest amongst the competing updates).
    *
    * @param key the key at which to update the cache
    * @param validAt ordering discriminator for pending updates for the same key
    * @param eventualValue the eventual result
    */
  final def putAsync(key: K, validAt: Long, eventualValue: Future[V])(implicit
      loggingContext: LoggingContext
  ): Future[Unit] = pendingUpdates.synchronized {
    val pendingUpdatesForKey = pendingUpdates.getOrElseUpdate(key, PendingUpdates.empty)
    if (pendingUpdatesForKey.latestValidAt < validAt) {
      pendingUpdatesForKey.latestValidAt = validAt
      pendingUpdatesForKey.pendingCount += 1
      registerEventualCacheUpdate(key, eventualValue, validAt)
    } else Future.unit
  }

  private def registerEventualCacheUpdate(
      key: K,
      eventualUpdate: Future[V],
      validAt: Long,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    eventualUpdate.transform[Unit](
      (value: V) =>
        pendingUpdates.synchronized {
          if (pendingUpdates(key).latestValidAt == validAt) {
            cache.put(key, value)
          }
          removeFromPending(key)
        },
      (err: Throwable) =>
        pendingUpdates.synchronized {
          removeFromPending(key)
          logger.warn(s"Failure in pending cache update for key $key", err)
          err
        },
    )

  private def removeFromPending(key: K): Unit =
    discard(pendingUpdates.synchronized {
      val pendingUpdateForKey = pendingUpdates(key)
      pendingUpdateForKey.pendingCount -= 1
      if (pendingUpdateForKey.pendingCount == 0L) {
        pendingUpdates -= key
      }
    })
}

object StateCache {

  /** Used to track competing updates to the cache for a specific key.
    * @param pendingCount The number of in-progress updates.
    * @param latestValidAt Highest version of any pending update.
    */
  private[cache] case class PendingUpdates(
      var pendingCount: Long,
      var latestValidAt: Long,
  )
  private[cache] object PendingUpdates {
    def empty: PendingUpdates = PendingUpdates(
      0L,
      Long.MinValue,
    )
  }
}
