// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.codahale.metrics.Timer
import com.daml.caching.{Cache, SizedCache}
import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.cache.MutableCacheBackedContractStore.ContractReadThroughNotFound
import com.daml.platform.store.cache.StateCache.PendingUpdatesState
import com.daml.scalautil.Statement.discard

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object ContractKeyStateCache {
  def apply(initialCacheIndex: Offset, cacheSize: Long, metrics: Metrics)(implicit
      ec: ExecutionContext
  ): ContractKeyStateCache =
    new ContractKeyStateCache(
      initialCacheIndex = initialCacheIndex,
      cache = SizedCache.from[java.lang.Long, Vector[ContractId]](
        SizedCache.Configuration(cacheSize),
        metrics.daml.execution.cache.keyState,
      ),
      registerUpdateTimer = metrics.daml.execution.cache.registerCacheUpdate,
    )
}

private[platform] case class ContractKeyStateCache(
    initialCacheIndex: Offset,
    cache: Cache[java.lang.Long, Vector[ContractId]],
    registerUpdateTimer: Timer,
)(implicit ec: ExecutionContext) {
  private val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private[cache] val pendingUpdates = mutable.Map.empty[Long, PendingUpdatesState]
  @volatile private[cache] var cacheIndex = initialCacheIndex

  def get(key: Long)(implicit loggingContext: LoggingContext): Option[Vector[ContractId]] =
    cache.getIfPresent(key) match {
      case Some(value) =>
        logger.debug(s"Cache hit for $key -> ${truncateValueForLogging(value)}")
        Some(value)
      case None =>
        logger.debug(s"Cache miss for $key ")
        None
    }

  def putBatch(
      validAt: Offset,
      newMappings: Iterable[(Long, ContractId)],
      removedMappings: Iterable[(Long, ContractId)],
  )(implicit loggingContext: LoggingContext): Unit =
    Timed.value(
      registerUpdateTimer, {
        pendingUpdates.synchronized {
          // The mutable contract state cache update stream should generally increase the cacheIndex strictly monotonically.
          // However, the most recent updates can be replayed in case of failure of the mutable contract state cache update stream.
          // In this case, we must ignore the already seen updates (i.e. that have `validAt` before or at the cacheIndex).
          if (validAt > cacheIndex) {
            newMappings.iterator
              .++(removedMappings.iterator)
              .map(_._1)
              .map(pendingUpdates.get)
              .flatMap(_.iterator)
              .foreach(_.latestValidAt = validAt)
            cacheIndex = validAt
            newMappings.iterator.foreach { case (keyHash, contractId) =>
              cache
                .getIfPresent(keyHash)
                .iterator
                .filter(!_.exists(_ == contractId))
                .map(_ :+ contractId)
                .foreach(cache.put(keyHash, _))
            }
            removedMappings.iterator.foreach { case (keyHash, contractId) =>
              cache
                .getIfPresent(keyHash)
                .iterator
                .filter(_.exists(_ == contractId))
                .map(_.filterNot(_ == contractId))
                .foreach(cache.put(keyHash, _))
            }
          } else
            logger.warn(
              s"Ignoring incoming synchronous update at an index ($validAt) equal to or before the cache index ($cacheIndex)"
            )
        }
      },
    )

  def putAsync(key: Long, fetchAsync: Offset => Future[Vector[ContractId]])(implicit
      loggingContext: LoggingContext
  ): Future[Vector[ContractId]] = Timed.value(
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

  def reset(resetAtOffset: Offset): Unit =
    pendingUpdates.synchronized {
      cacheIndex = resetAtOffset
      pendingUpdates.clear()
      cache.invalidateAll()
    }

  private def registerEventualCacheUpdate(
      key: Long,
      eventualUpdate: Future[Vector[ContractId]],
      validAt: Offset,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    eventualUpdate
      .map { (value: Vector[ContractId]) =>
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

  private def removeFromPending(key: Long)(implicit loggingContext: LoggingContext): Unit =
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

  private def truncateValueForLogging(value: Any) = {
    val stringValueRepr = value.toString
    val maxValueLength = 250
    if (stringValueRepr.length > maxValueLength)
      stringValueRepr.take(maxValueLength) + "..."
    else stringValueRepr
  }
}
