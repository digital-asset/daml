package com.daml.platform.store

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import com.daml.caching.{Cache, SizedCache}
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.participant.state.v1.Offset
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
import scala.util.Success

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
              eventOffset,
              _,
            ) =>
          globalKey.foreach(keyCache.put(_, Some(contractId -> flatEventWitnesses)))
          eventOffset
        case ContractLifecycleEvent.Archived(_, _, globalKey, _, _, eventOffset, _) =>
          globalKey.foreach(keyCache.put(_, Option.empty))
          eventOffset
      }
      .runForeach(currentOffset.set)
      .foreach { _ => println("Streaming cache updater finished") }

  private val currentOffset = new AtomicReference[Offset](Offset.beforeBegin)

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
    val currentCacheOffset = currentOffset.get()
    val promise = Promise[Option[(GlobalKey, Option[(ContractId, Set[Party])])]]
    feedAsync(promise.future)

    store
      .lookupContractKey(key)
      .andThen { case Success((maybeLastCreated, maybeLastDeleted)) =>
        (maybeLastCreated, maybeLastDeleted) match {
          case (Some(lastCreated), Some(lastDeleted))
              if lastCreated._1 <= currentCacheOffset && lastDeleted._1 <= currentCacheOffset =>
            promise.complete(Success(Some((key, Option.empty))))
          case (Some((createdAt, contractId, parties)), _) if createdAt <= currentCacheOffset =>
            promise.complete(Success(Some((key, Some(contractId -> parties)))))
//          case (None, _) => keyCache.put(key, Option.empty) // TDT Tricky one: can it cause incorrect cache state? Slim chances
          case _ => promise.complete(Success(Option.empty))
        }
      }
      .map { case (lastCreate, lastArchive) =>
        lastArchive
          .map { _ => Option.empty }
          .getOrElse {
            lastCreate.map { case (_, id, parties) =>
              id -> parties
            }
          }
      }
  }

  def feed(key: GlobalKey, contractDetails: Option[(ContractId, Set[Party])]): Unit =
    feedAsync(Future.successful(Some(key -> contractDetails)))

  private def feedAsync(
      future: Future[Option[(GlobalKey, Option[(ContractId, Set[Party])])]]
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
      .queue[Future[Option[(GlobalKey, Option[(ContractId, Set[Party])])]]](
        0,
        OverflowStrategy.dropNew,
        1,
      )
      .mapAsync(1000)(identity)
      .map {
        case Some((key, maybeValue)) =>
          keyCache.put(key, maybeValue)
        case None => ()
      }
      .toMat(Sink.ignore)(Keep.left)
      .run()

  private def `intersection non-empty`[T](one: Set[T], other: Set[T]): Boolean =
    one.toStream.intersect(other.toStream).nonEmpty

  def lookupContractKey(key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[(Option[(Offset, ContractId, Set[Party])], Option[(Offset, ContractId)])] =
    Future.successful(Option.empty -> Option.empty)
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
