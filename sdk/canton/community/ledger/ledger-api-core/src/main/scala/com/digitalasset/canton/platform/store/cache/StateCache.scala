// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle.Timer
import com.digitalasset.canton.caching.Cache
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.cache.StateCache.PendingUpdatesState
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}

/** This class is a wrapper around a Caffeine cache designed to handle correct resolution of
  * concurrent updates for the same key.
  *
  * The [[StateCache]] tracks its own notion of logical time with the `cacheIndex`
  * which evolves monotonically based on the index DB's offset (updated by [[putBatch]]).
  *
  * The cache's logical time (i.e. the `cacheIndex`) is used for establishing precedence of cache updates
  * stemming from read-throughs triggered from command interpretation on cache misses.
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
private[platform] case class StateCache[K, V](
    initialCacheIndex: Option[Offset],
    emptyLedgerState: V,
    cache: Cache[K, V],
    registerUpdateTimer: Timer,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  private[cache] val pendingUpdates = mutable.Map.empty[K, PendingUpdatesState]
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private[cache] var cacheIndex = initialCacheIndex

  /** Fetch the corresponding value for an input key, if present.
    *
    * @param key the key to query for
    * @return optionally [[V]]
    */
  def get(key: K)(implicit traceContext: TraceContext): Option[V] =
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
  def putBatch(validAt: Offset, batch: Map[K, V])(implicit
      traceContext: TraceContext
  ): Unit =
    Timed.value(
      registerUpdateTimer,
      blocking(pendingUpdates.synchronized {
        // The mutable contract state cache update stream should generally increase the cacheIndex strictly monotonically.
        // However, the most recent updates can be replayed in case of failure of the mutable contract state cache update stream.
        // In this case, we must ignore the already seen updates (i.e. that have `validAt` before or at the cacheIndex).
        if (Option(validAt) > cacheIndex) {
          batch.keySet.foreach { key =>
            pendingUpdates.updateWith(key)(_.map(_.withValidAt(validAt))).discard
          }
          cacheIndex = Some(validAt)
          cache.putAll(batch)
          logger.debug(
            s"Updated cache with a batch of ${batch
                .map { case (k, v) => s"$k -> ${truncateValueForLogging(v)}" }
                .mkString("[", ", ", "]")} at $validAt"
          )
        } else
          logger.warn(
            s"Ignoring incoming synchronous update at an index (${validAt.unwrap}) equal to or before the cache index (${cacheIndex
                .fold(0L)(_.unwrap)})"
          )
      }),
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
  @SuppressWarnings(Array("com.digitalasset.canton.SynchronizedFuture"))
  def putAsync(key: K, fetchAsync: Offset => Future[V])(implicit
      traceContext: TraceContext
  ): Future[V] =
    Timed.value(
      registerUpdateTimer,
      blocking(pendingUpdates.synchronized {
        cacheIndex match {
          case Some(validAt) =>
            val eventualValue = Future.delegate(fetchAsync(validAt))
            pendingUpdates.get(key) match {
              case Some(freshPendingUpdate) if freshPendingUpdate.latestValidAt == validAt =>
                eventualValue

              case Some(freshPendingUpdate) if freshPendingUpdate.latestValidAt > validAt =>
                ErrorUtil.invalidState(
                  s"Pending update ($freshPendingUpdate) should never be later than the cacheIndex ($validAt)."
                )

              case outdatedOrNew =>
                pendingUpdates
                  .put(
                    key,
                    PendingUpdatesState(
                      outdatedOrNew.map(_.pendingCount).getOrElse(0L) + 1L,
                      validAt,
                    ),
                  )
                  .discard
                registerEventualCacheUpdate(key, eventualValue, validAt)
                  .flatMap(_ => eventualValue)
            }

          case None =>
            Future.successful(emptyLedgerState)
        }
      }),
    )

  /** Resets the cache and cancels are pending asynchronous updates.
    *
    * @param resetAtOffset The cache re-initialization offset
    */
  def reset(resetAtOffset: Option[Offset]): Unit =
    blocking(pendingUpdates.synchronized {
      cacheIndex = resetAtOffset
      pendingUpdates.clear()
      cache.invalidateAll()
    })

  private def registerEventualCacheUpdate(
      key: K,
      eventualUpdate: Future[V],
      validAt: Offset,
  )(implicit traceContext: TraceContext): Future[Unit] =
    eventualUpdate
      .map { (value: V) =>
        Timed.value(
          registerUpdateTimer,
          blocking(pendingUpdates.synchronized {
            pendingUpdates.get(key) match {
              case Some(pendingForKey) =>
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
              case None =>
                logger.warn(
                  s"Pending updates tracker for $key not registered. This could be due to a transient error causing a restart in the index service."
                )
            }
          }),
        )
      }
      .recover { case err =>
        blocking(
          pendingUpdates.synchronized(
            removeFromPending(key)
          )
        )
        logger.info(s"Failure in pending cache update for key $key", err)
      }

  private def removeFromPending(key: K)(implicit traceContext: TraceContext): Unit =
    pendingUpdates
      .updateWith(key) {
        case Some(stillPending) if stillPending.pendingCount > 1 =>
          Some(stillPending.decPendingCount)

        case Some(lastPending) =>
          None

        case None =>
          logger.error(s"Expected pending updates tracker for key $key is missing")
          None
      }
      .discard

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
  private[cache] final case class PendingUpdatesState(
      pendingCount: Long,
      latestValidAt: Offset,
  ) {
    def withValidAt(validAt: Offset): PendingUpdatesState =
      this.copy(latestValidAt = validAt)
    def decPendingCount: PendingUpdatesState = this.copy(pendingCount = pendingCount - 1)
  }
}
