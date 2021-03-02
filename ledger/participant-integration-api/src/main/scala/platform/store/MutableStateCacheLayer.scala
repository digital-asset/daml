package com.daml.platform.store

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import com.daml.caching.{Cache, SizedCache}
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.MutableStateCacheLayer.KeyCacheValue
import com.daml.platform.store.dao.events.ContractLifecycleEventsReader.ContractLifecycleEvent

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

class MutableStateCacheLayer(
    store: ContractStore,
    keyCache: Cache[GlobalKey, KeyCacheValue],
)(implicit
    executionContext: ExecutionContext
) extends ContractStore {
  private val actorSystem = ActorSystem("mutable-state-cache")
  implicit private val materializer: Materializer = Materializer(actorSystem)
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = { val _ = Await.ready(actorSystem.terminate(), 10.seconds) }
  })

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
          globalKey.foreach(feed(_, Some(contractId -> flatEventWitnesses)))
          eventSequentialId
        case archived: ContractLifecycleEvent.Archived =>
          archived.globalKey.foreach(feed(_, Option.empty))
          archived.eventSequentialId
      }
      .runForeach(currentCacheIndex.set)
      .foreach { _ => println("Streaming cache updater finished") }

  private val currentCacheIndex = new AtomicLong(0L)

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

  private def fetch(key: GlobalKey, readers: Set[Party] /* Use this */ )(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    Future
      .successful(keyCache.getIfPresent(key))
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
    val currentCacheOffset = currentCacheIndex.get()
    val promise = Promise[Option[(Long, ContractId, Set[Party], Option[Long])]]

    feedAsync(() => {
      promise.future.map {
        case Some((createdAt, _, _, Some(archivedAt)))
            if archivedAt < currentCacheOffset && createdAt < currentCacheOffset =>
          Some(key -> Option.empty[(ContractId, Set[Party])])
        case Some((createdAt, contractId, parties, _)) if createdAt < currentCacheOffset =>
          Some(key -> Some(contractId -> parties))
        case None => None
      }
    })

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

  def feed(key: GlobalKey, contractDetails: Option[(ContractId, Set[Party])]): Unit =
    feedAsync(() => Future.successful(Some(key -> contractDetails)))

  private def feedAsync(
      future: () => Future[Option[(GlobalKey, Option[(ContractId, Set[Party])])]]
  ): Unit =
    queue
      .offer(future)
      .foreach {
        case QueueOfferResult.Failure(err) =>
          println(s"Offering failed: ${err.getMessage}")
        case QueueOfferResult.QueueClosed =>
          println("Queue is closed. Not accepting anymore offers")
        case QueueOfferResult.Enqueued =>
        case QueueOfferResult.Dropped =>
          println("Mutable state cache queue offer dropped")
      }

  private val queue =
    Source
      .queue[() => Future[Option[(GlobalKey, Option[(ContractId, Set[Party])])]]](
        0,
        OverflowStrategy.dropNew,
        1,
      )
      .mapAsync(1000)(f => f())
      .map {
        case Some((key, maybeValue)) =>
          keyCache.put(key, maybeValue)
        case None => ()
      }
      .toMat(Sink.ignore)(Keep.left)
      .run()

  private def `intersection non-empty`[T](one: Set[T], other: Set[T]): Boolean =
    one.toStream.intersect(other.toStream).nonEmpty

  override def lookupContractKey(key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Long, ContractId, Set[Party], Option[Long])]] =
    Future.failed(new RuntimeException("should not go through here"))
}

object MutableStateCacheLayer {
  type KeyCacheValue = Option[(ContractId, Set[Party])]

  def apply(store: ContractStore, metrics: Metrics)(implicit
      executionContext: ExecutionContext
  ): MutableStateCacheLayer =
    new MutableStateCacheLayer(
      store,
      SizedCache.from(SizedCache.Configuration(10000L), metrics.daml.execution.keyStateCache),
    )
}
