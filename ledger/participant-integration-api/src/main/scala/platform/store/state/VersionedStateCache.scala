package com.daml.platform.store.state

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.daml.platform.store.state.StateCache.PendingUpdates

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait VersionedStateCache[K, V] {
  private[store] val version = new AtomicLong(0L)
  private[store] val backingCache = mutable.Map.empty[K, (Long, V)]
  private[store] val pendingUpdates =
    collection.concurrent.TrieMap.empty[K, PendingUpdates]

  protected implicit def ec: ExecutionContext

  /* Update the cache at $key with $value if $validAt increased over the last cache update */
  def put(key: K, validAt: Long, value: V): Boolean =
    if (version.updateAndGet(v => if (v < validAt) validAt else v) == validAt) {
      pendingUpdates.get(key).foreach { pending =>
        /* Assumes that puts are called monotonically and putAsync always has validAt = */
        pending.synchronized(pending.latestRef.set(validAt))
      }
      backingCache.put(key, (validAt, value))
      true
    } else false

  /* Eventually update the cache at $key with the result of $eventualValue
   * if $validAt increased over the last cache update */
  def putAsync(key: K, validAt: Long, eventualValue: Future[V]): Unit =
    if (version.updateAndGet(v => if (v < validAt) validAt else v) == validAt) {
      val pendingUpdate @ PendingUpdates(pendingCountRef, latestRef, effectsChain) =
        pendingUpdates.getOrElseUpdate(key, PendingUpdates.empty)

      /* We need to synchronize here instead of using a lock-free update
       * since enqueueing the next update in the chain is impure
       * and cannot be retried/cancelled on thread contention.
       */
      val successful = pendingUpdate.synchronized {
        /* Invalidated by a more recent put or a more recent pending update */
        if (latestRef.get() >= validAt) false
        /* Is this the first update in this effect chain? */
        else if (latestRef.get() == Long.MinValue) {
          pendingCountRef.set(1L)
          latestRef.set(validAt)
          effectsChain
            .updateAndGet(_.transformWith { _ =>
              registerEventualCacheUpdate(key, eventualValue, validAt).map(_ => ())
            })
          true
        } else if (othersPending(pendingCountRef, latestRef)) {
          /* Double check that a effect completion did not proceed to delete the entry in the meantime
           * If 0 is returned it means another update is going to delete this entry,
           * we need to retry.
           * */
          if (pendingCountRef.getAndIncrement() > 0L) {
            latestRef.set(validAt)
            effectsChain.updateAndGet(_.transformWith { _ =>
              registerEventualCacheUpdate(key, eventualValue, validAt).map(_ => ())
            })
            true
          } else {
            latestRef.get() < validAt
          }
        } else true
      }
      if (!successful) putAsync(key, validAt, eventualValue) else ()
    }

  private def othersPending(pendingCount: AtomicLong, highestIndex: AtomicLong) =
    highestIndex.get() >= 0L && pendingCount.get() > 0L

  private def registerEventualCacheUpdate(
      key: K,
      eventualUpdate: Future[V],
      validAt: Long,
  ): Future[V] =
    eventualUpdate.andThen {
      case Success(update) =>
        // Double-check if we need to update
        if (pendingUpdates(key).latestRef.get() == validAt) {
          backingCache.put(key, (validAt, update))
        }
        removeFromPending(key)
      case Failure(_) =>
        removeFromPending(key)
    }

  private def removeFromPending(key: K): Unit =
    if (pendingUpdates(key).pendingCount.decrementAndGet() == 0L) pendingUpdates -= key

  /* Fetch the entry for K together with the time when it was inserted */
  def get(key: K): Option[(Long, V)] = backingCache.get(key)
}

object VersionedStateCache {
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
