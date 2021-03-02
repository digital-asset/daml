package com.daml.platform.store.state

import java.time.Instant
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.caching.{Cache, SizedCache}
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.dao.events.ContractLifecycleEventsReader.ContractLifecycleEvent
import MutableStateCacheLayer._

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success

private[store] class MutableStateCacheLayer private (
    metrics: Metrics,
    store: ContractStore,
    keyCache: Cache[GlobalKey, KeyCacheValue],
)(implicit
    executionContext: ExecutionContext,
    materializer: Materializer,
) extends ContractStore {
  private val currentCacheIndex = new AtomicLong(0L)
  private val updatesPendingMap =
    mutable.Map.empty[GlobalKey, (AtomicLong, AtomicLong, AtomicReference[Future[KeyStateUpdate]])]

  def consumeFrom(contractEvents: Source[ContractLifecycleEvent, NotUsed]): Unit =
    contractEvents
      .map {
        case ContractLifecycleEvent.Created(
              _,
              contractId,
              globalKey,
              flatEventWitnesses,
              _,
              eventSequentialId,
            ) =>
          globalKey.foreach(
            feedKey(
              _,
              eventSequentialId,
              Assigned(contractId, flatEventWitnesses, eventSequentialId),
            )
          )
          eventSequentialId
        case archived: ContractLifecycleEvent.Archived =>
          archived.globalKey.foreach(
            feedKey(_, archived.eventSequentialId, Unassigned(archived.eventSequentialId))
          )
          archived.eventSequentialId
      }
      .runForeach(idx => {
        currentCacheIndex.set(idx)
        metrics.daml.indexer.currentStateCacheSequentialIdGauge.updateValue(idx)
      })
      .foreach { _ => println("Streaming cache updater finished") } // TDT remove println

  private def fetch(key: GlobalKey, readers: Set[Party] /* Use this */ )(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    Future
      .successful(keyCache.getIfPresent(key))
      .andThen { case Success(Some(cacheHit)) =>
        println(s"Cache hit at ${currentCacheIndex.get()}: $key -> $cacheHit")
      } // TDT remove println
      .flatMap {
        case None =>
          readThroughCache(key).map(_.collect {
            case (id, parties) if `intersection non-empty`(readers, parties) => id
          })
        case Some(Some((contractId, parties))) if `intersection non-empty`(readers, parties) =>
          Future.successful(Some(contractId))
        case _ => Future.successful(None)
      }

  private def readThroughCache(
      key: GlobalKey
  )(implicit loggingContext: LoggingContext): Future[Option[(ContractId, Set[Party])]] = {
    // Get here so we can close over this immutable `state in time` in the future below.
    val currentCacheOffset = currentCacheIndex.get()
    val promise = Promise[Option[(Long, ContractId, Set[Party], Option[Long])]]

    feedKeyAsync(() =>
      (
        key,
        currentCacheOffset,
        promise.future.map {
          case Some((createdAt, _, _, Some(archivedAt)))
              if archivedAt < currentCacheOffset && createdAt < currentCacheOffset =>
            Unassigned(currentCacheOffset)
          case Some((createdAt, contractId, parties, _)) if createdAt < currentCacheOffset =>
            Assigned(contractId, parties, currentCacheOffset)
          case None =>
            // Safe to update since there cannot be any pending updates
            // at an earlier stage conflicting with this one  (e.g. the key is never assigned previous to this cache offset).
            Unassigned(currentCacheOffset)
        },
      )
    )

    val eventualResult = store.lookupContractKey(key)
    promise.completeWith(eventualResult)

    eventualResult.map {
      case Some((_, _, _, Some(_))) =>
        // Archived
        Option.empty[(ContractId, Set[Party])]
      case Some((_, contractId, parties, None)) => Some(contractId -> parties)
      case None => Option.empty[(ContractId, Set[Party])]
    }
  }

  private def feedKey(key: GlobalKey, validAt: Long, keyStateUpdate: KeyStateUpdate): Unit =
    feedKeyAsync(() => (key, validAt, Future.successful(keyStateUpdate)))

  private def toKeyCacheValue(keyStateUpdate: KeyStateUpdate): KeyCacheValue =
    keyStateUpdate match {
      case Unassigned(_) => Option.empty[(ContractId, Set[Party])]
      case Assigned(contractId, createWitnesses, _) =>
        Some(contractId -> createWitnesses)
    }

  private def feedKeyAsync(
      future: () => (GlobalKey, Long, Future[KeyStateUpdate])
  ): Unit = {
    val (key, validAt, pendingUpdate) = future()
    val start = System.nanoTime()
    updatesPendingMap.synchronized {
      updatesPendingMap
        .get(key) match {
        case Some((concurrentUpdatesReference, highestIndex, newUpdate)) =>
          if (validAt > highestIndex.get()) {
            highestIndex.set(validAt)
            concurrentUpdatesReference.incrementAndGet()
            newUpdate.getAndUpdate(_.transformWith { _ =>
              registerEventualCacheUpdate(key, pendingUpdate)
            })
            ()
          } else ()
        case None =>
          updatesPendingMap.put(
            key,
            (
              new AtomicLong(1L),
              new AtomicLong(validAt),
              new AtomicReference[Future[KeyStateUpdate]](
                registerEventualCacheUpdate(key, pendingUpdate)
              ),
            ),
          )
      }
    }
    println(
      s"Synchronizing over updates pending map took ${(System.nanoTime() - start) / 1000L} micros" // TDT remove println
    )
  }

  private def registerEventualCacheUpdate(
      key: GlobalKey,
      eventualKeyUpdate: Future[KeyStateUpdate],
  ): Future[KeyStateUpdate] =
    eventualKeyUpdate.andThen { case Success(keyStateUpdate) =>
      println(s"Updating cache with $key -> $keyStateUpdate")
      keyCache.put(key, toKeyCacheValue(keyStateUpdate))
      updatesPendingMap.synchronized {
        // Cleanup
        // We cannot lose the reference here
        if (updatesPendingMap(key)._1.decrementAndGet() == 0L)
          updatesPendingMap -= key
      }
    }

  private def `intersection non-empty`[T](one: Set[T], other: Set[T]): Boolean =
    one.toStream.intersect(other.toStream).nonEmpty

  override def lookupActiveContract(readers: Set[Party], contractId: ContractId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Value.ContractInst[Value.VersionedValue[ContractId]]]] =
    store.lookupActiveContract(readers, contractId)

  override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    fetch(key, readers)

  override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Instant]] =
    store.lookupMaximumLedgerTime(ids)

  override def lookupContractKey(key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Long, ContractId, Set[Party], Option[Long])]] =
    Future.failed(new RuntimeException("should not go through here"))
}

object MutableStateCacheLayer {
  type KeyCacheValue = Option[(ContractId, Set[Party])]

  sealed trait KeyStateUpdate extends Product with Serializable {
    def asOf: Long // Consider removing
  }
  final case class Unassigned(asOf: Long) extends KeyStateUpdate
  final case class Assigned(contractId: ContractId, createWitnesses: Set[Party], asOf: Long)
      extends KeyStateUpdate

  def apply(store: ContractStore, metrics: Metrics)(implicit
      executionContext: ExecutionContext,
      mat: Materializer,
  ): MutableStateCacheLayer =
    new MutableStateCacheLayer(
      metrics,
      store,
      SizedCache.from(SizedCache.Configuration(10000L), metrics.daml.execution.keyStateCache),
    )
}
