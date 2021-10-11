// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.codahale.metrics.Timer
import com.daml.caching.Cache
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Timed
import com.daml.platform.store.cache.StateCache.PendingUpdatesState
import com.daml.scalautil.Statement.discard

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** This class is a wrapper around a Caffeine cache designed to handle
  * correct resolution of concurrent updates for the same key.
  */
private[platform] case class StateCache[K, V](cache: Cache[K, V], registerUpdateTimer: Timer)(
    implicit ec: ExecutionContext
) {
  private val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private[cache] val pendingUpdates = mutable.Map.empty[K, PendingUpdatesState]

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
  def put(key: K, validAt: Long, value: V): Unit = Timed.value(
    registerUpdateTimer, {
      pendingUpdates.synchronized {
        val competingLatestForKey =
          pendingUpdates
            .get(key)
            .map { pendingUpdate =>
              val oldLatestValidAt = pendingUpdate.latestValidAt
              pendingUpdate.latestValidAt = Math.max(validAt, pendingUpdate.latestValidAt)
              oldLatestValidAt
            }
            .getOrElse(Long.MinValue)

        if (competingLatestForKey < validAt) cache.put(key, value) else ()
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
    * @param validAt ordering discriminator for pending updates for the same key
    * @param eventualValue the eventual result signaling successful enqueuing of the cache async update
    */
  final def putAsync(key: K, validAt: Long, eventualValue: Future[V])(implicit
      loggingContext: LoggingContext
  ): Future[Unit] = Timed.value(
    registerUpdateTimer,
    pendingUpdates.synchronized {
      val pendingUpdatesForKey = pendingUpdates.getOrElseUpdate(key, PendingUpdatesState.empty)
      if (pendingUpdatesForKey.latestValidAt < validAt) {
        pendingUpdatesForKey.latestValidAt = validAt
        pendingUpdatesForKey.pendingCount += 1
        registerEventualCacheUpdate(key, eventualValue, validAt)
      } else Future.unit
    },
  )

  private def registerEventualCacheUpdate(
      key: K,
      eventualUpdate: Future[V],
      validAt: Long,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    eventualUpdate
      .map { (value: V) =>
        pendingUpdates.synchronized {
          pendingUpdates
            .get(key)
            .map { pendingForKey =>
              if (pendingForKey.latestValidAt == validAt)
                cache.put(key, value)
              else ()
              removeFromPending(key)
            }
            .getOrElse(logger.error(s"Pending updates tracker for $key not registered "))
        }
      }
      .recover { case err =>
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
}

object StateCache {

  /** Used to track competing updates to the cache for a specific key.
    * @param pendingCount The number of in-progress updates.
    * @param latestValidAt Highest version of any pending update.
    */
  private[cache] case class PendingUpdatesState(
      var pendingCount: Long,
      var latestValidAt: Long,
  )
  private[cache] object PendingUpdatesState {
    def empty: PendingUpdatesState = PendingUpdatesState(
      0L,
      Long.MinValue,
    )
  }
}
