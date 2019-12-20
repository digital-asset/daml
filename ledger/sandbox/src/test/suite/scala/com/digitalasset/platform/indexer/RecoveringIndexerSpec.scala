// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorSystem
import akka.pattern.after
import ch.qos.logback.classic.Level
import com.digitalasset.platform.indexer.TestIndexer._
import com.digitalasset.platform.sandbox.logging.TestNamedLoggerFactory
import org.scalatest.BeforeAndAfterEach
import com.digitalasset.dec.DirectExecutionContext
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}

class TestIndexer(results: Iterator[SubscribeResult]) {
  private[this] val actorSystem = ActorSystem("TestIndexer")
  private[this] val scheduler = actorSystem.scheduler

  val actions = new ConcurrentLinkedQueue[IndexerEvent]()

  class TestIndexerFeedHandle(result: SubscribeResult)(implicit executionContext: ExecutionContext)
      extends IndexFeedHandle {
    private[this] val promise = Promise[akka.Done]()

    if (result.status == SuccessfullyCompletes) {
      scheduler.scheduleOnce(result.completeDelay)({
        actions.add(EventStreamComplete(result.name))
        promise.trySuccess(akka.Done)
        ()
      })
    } else {
      scheduler.scheduleOnce(result.completeDelay)({
        actions.add(EventStreamFail(result.name))
        promise.tryFailure(new RuntimeException("Random simulated failure: subscribe"))
        ()
      })
    }

    override def stop(): Future[akka.Done] = {
      actions.add(EventStopCalled(result.name))
      promise.trySuccess(akka.Done)
      promise.future
    }

    override def completed(): Future[akka.Done] = {
      promise.future
    }
  }

  def subscribe()(implicit executionContext: ExecutionContext): Future[IndexFeedHandle] = {
    val result = results.next()
    actions.add(EventSubscribeCalled(result.name))
    if (result.status != SubscriptionFails) {
      after(result.subscribeDelay, scheduler)({
        actions.add(EventSubscribeSuccess(result.name))
        Future.successful(new TestIndexerFeedHandle(result))
      })
    } else {
      after(result.subscribeDelay, scheduler)({
        actions.add(EventSubscribeFail(result.name))
        Future.failed(new RuntimeException("Random simulated failure: subscribe"))
      })
    }
  }
}

object TestIndexer {
  case class SubscribeResult(
      name: String,
      status: SubscribeStatus,
      subscribeDelay: FiniteDuration,
      completeDelay: FiniteDuration,
  )

  sealed abstract class IndexerEvent {
    def name: String
  }
  final case class EventStreamFail(name: String) extends IndexerEvent
  final case class EventStreamComplete(name: String) extends IndexerEvent
  final case class EventStopCalled(name: String) extends IndexerEvent
  final case class EventSubscribeCalled(name: String) extends IndexerEvent
  final case class EventSubscribeSuccess(name: String) extends IndexerEvent
  final case class EventSubscribeFail(name: String) extends IndexerEvent

  sealed trait SubscribeStatus
  case object StreamFails extends SubscribeStatus
  case object SubscriptionFails extends SubscribeStatus
  case object SuccessfullyCompletes extends SubscribeStatus
}

class RecoveringIndexerSpec extends AsyncWordSpec with Matchers with BeforeAndAfterEach {

  private[this] implicit val executionContext: ExecutionContext = DirectExecutionContext
  private[this] val actorSystem = ActorSystem("RecoveringIndexerIT")
  private[this] val scheduler = actorSystem.scheduler
  private[this] val loggerFactory = TestNamedLoggerFactory(getClass)

  override def afterEach(): Unit = {
    loggerFactory.cleanup()
    super.afterEach()
  }

  "RecoveringIndexer" should {

    "work when the stream completes" in {
      val recoveringIndexer =
        new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second, loggerFactory)
      val testIndexer = new TestIndexer(
        List(
          SubscribeResult("A", SuccessfullyCompletes, 10.millis, 10.millis)
        ).iterator)

      val end = recoveringIndexer.start(() => testIndexer.subscribe())
      end map { _ =>
        List(testIndexer.actions.toArray: _*) should contain theSameElementsInOrderAs List[
          IndexerEvent](
          EventSubscribeCalled("A"),
          EventSubscribeSuccess("A"),
          EventStreamComplete("A"),
        )
        logs should be(
          Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
            Level.INFO -> "Successfully finished processing state updates",
          ))
      }
    }

    "work when the stream is stopped" in {
      val recoveringIndexer =
        new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second, loggerFactory)
      // Stream completes after 10sec, but stop() is called before
      val testIndexer = new TestIndexer(
        List(
          SubscribeResult("A", SuccessfullyCompletes, 10.millis, 10.seconds),
        ).iterator)

      val end = recoveringIndexer.start(() => testIndexer.subscribe())
      scheduler.scheduleOnce(100.millis, () => recoveringIndexer.close())

      end map { _ =>
        List(testIndexer.actions.toArray: _*) should contain theSameElementsInOrderAs List[
          IndexerEvent](
          EventSubscribeCalled("A"),
          EventSubscribeSuccess("A"),
          EventStopCalled("A"),
        )
        logs should be(
          Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
            Level.INFO -> "Successfully finished processing state updates",
          ))
      }
    }

    "recover failures" in {
      val recoveringIndexer =
        new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second, loggerFactory)
      // Subscribe fails, then the stream fails, then the stream completes without errors.
      val testIndexer = new TestIndexer(
        List(
          SubscribeResult("A", SubscriptionFails, 10.millis, 10.millis),
          SubscribeResult("B", StreamFails, 10.millis, 10.millis),
          SubscribeResult("C", SuccessfullyCompletes, 10.millis, 10.millis),
        ).iterator)

      val end = recoveringIndexer.start(() => testIndexer.subscribe())

      end map { _ =>
        List(testIndexer.actions.toArray: _*) should contain theSameElementsInOrderAs List[
          IndexerEvent](
          EventSubscribeCalled("A"),
          EventSubscribeFail("A"),
          EventSubscribeCalled("B"),
          EventSubscribeSuccess("B"),
          EventStreamFail("B"),
          EventSubscribeCalled("C"),
          EventSubscribeSuccess("C"),
          EventStreamComplete("C"),
        )
        logs should be(
          Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.ERROR -> "Error while running indexer, restart scheduled after 10 milliseconds",
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
            Level.ERROR -> "Error while running indexer, restart scheduled after 10 milliseconds",
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
            Level.INFO -> "Successfully finished processing state updates",
          ))
      }
    }

    "respect restart delay" in {
      val recoveringIndexer =
        new RecoveringIndexer(actorSystem.scheduler, 500.millis, 1.second, loggerFactory)
      // Subscribe fails, then the stream completes without errors. Note the restart delay of 500ms.
      val testIndexer = new TestIndexer(
        List(
          SubscribeResult("A", SubscriptionFails, 0.millis, 0.millis),
          SubscribeResult("B", SuccessfullyCompletes, 0.millis, 0.millis),
        ).iterator)

      val t0 = System.nanoTime()
      val end = recoveringIndexer.start(() => testIndexer.subscribe())
      end map { _ =>
        val t1 = System.nanoTime()

        (t1 - t0) should be >= 500.millis.toNanos
        List(testIndexer.actions.toArray: _*) should contain theSameElementsInOrderAs List[
          IndexerEvent](
          EventSubscribeCalled("A"),
          EventSubscribeFail("A"),
          EventSubscribeCalled("B"),
          EventSubscribeSuccess("B"),
          EventStreamComplete("B"),
        )
        logs should be(
          Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.ERROR -> "Error while running indexer, restart scheduled after 500 milliseconds",
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
            Level.INFO -> "Successfully finished processing state updates",
          ))
      }
    }
  }

  private def logs: Seq[TestNamedLoggerFactory.LogEvent] =
    loggerFactory.logs(classOf[RecoveringIndexer])
}
