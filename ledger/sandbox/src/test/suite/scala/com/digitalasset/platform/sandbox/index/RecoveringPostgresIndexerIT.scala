// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.{Done, NotUsed}
import akka.actor.{ActorSystem, Scheduler}
import akka.pattern.after
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.{LedgerInitialConditions, Offset, ReadService, Update}
import org.scalatest.{Matchers, WordSpec}
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

sealed abstract class StopResult
final case class StopFails(delay: FiniteDuration) extends StopResult
final case class StopSucceeds(delay: FiniteDuration) extends StopResult

sealed abstract class SubscribeResult
final case class SubscribeFails(delay: FiniteDuration) extends SubscribeResult
final case class SubscribeSucceeds(
    delay: FiniteDuration,
    stopResult: StopResult,
    failAfter: Option[FiniteDuration])
    extends SubscribeResult

sealed abstract class CreateResult
final case class CreateFails(delay: FiniteDuration) extends CreateResult
final case class CreateSucceeds(delay: FiniteDuration, subscribeResult: SubscribeResult)
    extends CreateResult

class TestIndexerFactory(creates: Iterator[CreateResult], scheduler: Scheduler) {

  var indexers: List[TestIndexer] = List.empty
  def activeIndexer: TestIndexer = indexers.head

  class TestIndexer(subscribes: Iterator[SubscribeResult]) extends Indexer with AutoCloseable {
    var closed = false
    var subscribed = false
    var crashed = false
    var errorHandler: Throwable => Unit = (_ => ())

    class TestIndexerFeedHandle(stops: Iterator[StopResult]) extends IndexFeedHandle {
      override def stop(): Future[Done] = synchronized {
        assert(subscribed, "Not subscribed")
        stops.next() match {
          case StopFails(delay) =>
            after(delay, scheduler)({
              Future.failed(new RuntimeException("Random simulated failure: stop"))
            })(DEC)
          case StopSucceeds(delay) =>
            after(delay, scheduler)({
              subscribed = false
              Future.successful(Done)
            })(DEC)
        }
      }
    }

    override def subscribe(
        readService: ReadService,
        onError: Throwable => Unit,
        onComplete: () => Unit): Future[IndexFeedHandle] = synchronized {
      assert(!subscribed, "Already subscribed")
      errorHandler = onError
      subscribes.next() match {
        case SubscribeFails(delay) =>
          after(delay, scheduler)({
            Future.failed(new RuntimeException("Random simulated failure: subscribe"))
          })(DEC)
        case SubscribeSucceeds(delay, stop, failAfterO) =>
          failAfterO.foreach(failAfter =>
            scheduler.scheduleOnce(failAfter)({
              crashed = true
              onError(new RuntimeException("Random simulated failure: onError"))
            })(DEC))
          after(delay, scheduler)({
            subscribed = true
            Future.successful(new TestIndexerFeedHandle(Iterator.single(stop)))
          })(DEC)

      }
    }

    override def close(): Unit = {
      assert(!closed, "Already closed")
      closed = true
    }
  }

  def create(): Future[Indexer with AutoCloseable] = synchronized {
    creates.next() match {
      case CreateFails(delay) =>
        after(delay, scheduler)({
          Future.failed(new RuntimeException("Random simulated failure: create"))
        })(DEC)
      case CreateSucceeds(delay, subscribe) =>
        after(delay, scheduler)({
          val indexer = new TestIndexer(Iterator.single(subscribe))
          indexers = indexer :: indexers
          Future.successful(indexer)
        })(DEC)
    }
  }
}

object TestReadService extends ReadService {
  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] = Source.empty
  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
    Source.empty
}

class RecoveringPostgresIndexerIT extends WordSpec with Matchers {

  private[this] val actorSystem = ActorSystem("PollingUtilsSpec")
  private[this] implicit val materializer: ActorMaterializer = ActorMaterializer()(actorSystem)
  private[this] implicit val scheduler: Scheduler = materializer.system.scheduler
  private[this] implicit val ec: ExecutionContext = DEC

  "RecoveringPostgresIndexer" should {

    /**
      * Test whether all operations (create, subscribe, stop, close) are forwarded to the implementation.
      */
    "support all operation" in {
      val factory: TestIndexerFactory = new TestIndexerFactory(
        List[CreateResult](
          CreateSucceeds(0.millis, SubscribeSucceeds(0.millis, StopSucceeds(0.millis), None))
        ).iterator,
        scheduler)

      Await.result(
        for {
          indexer <- RecoveringPostgresIndexer.create(() => factory.create(), 1.millis)
          handle <- indexer.subscribe(TestReadService, _ => (), () => ())
          _ = assert(factory.activeIndexer.subscribed)
          done <- handle.stop()
          _ = indexer.close()
        } yield done,
        10.seconds
      )

      factory.indexers.length shouldBe 1
      factory.indexers(0).subscribed shouldBe false
      factory.indexers(0).closed shouldBe true
    }

    "retry a failed create" in {
      val factory: TestIndexerFactory = new TestIndexerFactory(
        List[CreateResult](
          CreateFails(0.millis),
          CreateSucceeds(0.millis, SubscribeSucceeds(0.millis, StopSucceeds(0.millis), None))
        ).iterator,
        scheduler)

      Await.result(for {
        indexer <- RecoveringPostgresIndexer.create(() => factory.create(), 1.millis)
      } yield indexer, 10.seconds)

      factory.indexers.length shouldBe 1
    }

    "retry a failed subscribe" in {
      val factory: TestIndexerFactory = new TestIndexerFactory(
        List[CreateResult](
          CreateSucceeds(0.millis, SubscribeFails(0.millis)),
          CreateSucceeds(0.millis, SubscribeSucceeds(0.millis, StopSucceeds(0.millis), None))
        ).iterator,
        scheduler
      )

      Await.result(
        for {
          indexer <- RecoveringPostgresIndexer.create(() => factory.create(), 1.millis)
          handle <- indexer.subscribe(TestReadService, _ => (), () => ())
        } yield handle,
        10.seconds
      )

      factory.indexers.length shouldBe 2
      factory.indexers(1).subscribed shouldBe false
      factory.indexers(0).subscribed shouldBe true
    }

    /**
      * The wrapper hides internal failures.
      * Test whether stop() can be called while the underlying indexer is failed.
      *
      * 1. 0ms:   Indexer successfully created
      * 2. 0ms:   subscribe() called
      * 3. 10ms:  Indexer crashes
      * 4. 50ms:  stop() called (while no indexer is alive)
      * 4. 100ms: New indexer successfully created
      */
    "support operations in a failed state" in {
      val factory: TestIndexerFactory = new TestIndexerFactory(
        List[CreateResult](
          CreateSucceeds(
            0.millis,
            SubscribeSucceeds(0.millis, StopSucceeds(0.millis), Some(10.millis))),
          CreateSucceeds(100.millis, SubscribeSucceeds(0.millis, StopSucceeds(0.millis), None))
        ).iterator,
        scheduler
      )

      Await.result(
        for {
          indexer <- RecoveringPostgresIndexer.create(() => factory.create(), 1.millis)
          handle <- indexer.subscribe(TestReadService, _ => (), () => ())
          _ <- akka.pattern.after(50.millis, scheduler)(Future.successful(()))
          _ = assert(factory.activeIndexer.crashed)
          done <- handle.stop()
          _ <- akka.pattern.after(200.millis, scheduler)(Future.successful(()))
        } yield done,
        10.seconds
      )

      factory.indexers.length shouldBe 2
      factory.indexers(1).crashed shouldBe true
      factory.indexers(0).subscribed shouldBe false
    }

    /**
      * Test whether a subscribed indexer can recover from multiple failures:
      *
      * 1. Subscribe succeeds
      * 2. Random failure occurs
      * 3. Create fails
      * 4. Create succeeds, subscribe fails
      * 5. Everything succeeds
      */
    "recover from multiple failures" in {
      val factory: TestIndexerFactory = new TestIndexerFactory(
        List[CreateResult](
          CreateSucceeds(
            10.millis,
            SubscribeSucceeds(10.millis, StopSucceeds(10.millis), Some(20.millis))),
          CreateFails(10.millis),
          CreateSucceeds(10.millis, SubscribeFails(10.millis)),
          CreateSucceeds(10.millis, SubscribeSucceeds(10.millis, StopSucceeds(10.millis), None)),
        ).iterator,
        scheduler
      )

      Await.result(
        for {
          indexer <- RecoveringPostgresIndexer.create(() => factory.create(), 1.millis)
          handle <- indexer.subscribe(TestReadService, _ => (), () => ())
        } yield handle,
        10.seconds
      )

      factory.indexers.length shouldBe 3
      factory.indexers(2).crashed shouldBe true
      factory.indexers(1).subscribed shouldBe false
      factory.indexers(0).subscribed shouldBe true
    }
  }
}
