// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorSystem
import akka.pattern.after
import ch.qos.logback.classic.Level
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner, TestResourceContext}
import com.daml.logging.LoggingContext
import com.daml.platform.indexer.RecoveringIndexerSpec._
import com.daml.platform.testing.LogCollector
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

final class RecoveringIndexerSpec
    extends AsyncWordSpec
    with Matchers
    with TestResourceContext
    with BeforeAndAfterEach {

  private[this] implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private[this] var actorSystem: ActorSystem = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    actorSystem = ActorSystem(getClass.getSimpleName)
    LogCollector.clear[this.type]
  }

  override def afterEach(): Unit = {
    Await.result(actorSystem.terminate(), 10.seconds)
    super.afterEach()
  }

  private def readLog(): Seq[(Level, String)] = LogCollector.read[this.type, RecoveringIndexer]

  "RecoveringIndexer" should {
    "work when the stream completes" in {
      val recoveringIndexer = new RecoveringIndexer(actorSystem.scheduler, 10.millis)
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SuccessfullyCompletes, 10.millis, 10.millis),
      )

      val resource = recoveringIndexer.start(() => testIndexer.subscribe())
      resource.asFuture.flatten
        .transformWith(finallyRelease(resource))
        .map { _ =>
          testIndexer.actions shouldBe Seq[IndexerEvent](
            EventSubscribeCalled("A"),
            EventSubscribeSuccess("A"),
            EventStreamComplete("A"),
            EventStopCalled("A"),
          )
          readLog() should contain theSameElementsInOrderAs Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
            Level.INFO -> "Successfully finished processing state updates",
            Level.INFO -> "Stopping Indexer Server",
            Level.INFO -> "Stopped Indexer Server",
          )
          testIndexer.openSubscriptions shouldBe mutable.Set.empty
        }
    }

    "work when the stream is stopped" in {
      val recoveringIndexer = new RecoveringIndexer(actorSystem.scheduler, 10.millis)
      // Stream completes after 10s, but is released before that happens
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SuccessfullyCompletes, 10.millis, 10.seconds),
      )

      val resource = recoveringIndexer.start(() => testIndexer.subscribe())

      for {
        _ <- akka.pattern.after(100.millis, actorSystem.scheduler)(Future.unit)
        _ <- resource.release()
        complete <- resource.asFuture
        _ <- complete
      } yield {
        testIndexer.actions shouldBe Seq[IndexerEvent](
          EventSubscribeCalled("A"),
          EventSubscribeSuccess("A"),
          EventStopCalled("A"),
        )
        readLog() should contain theSameElementsInOrderAs Seq(
          Level.INFO -> "Starting Indexer Server",
          Level.INFO -> "Started Indexer Server",
          Level.INFO -> "Stopping Indexer Server",
          Level.INFO -> "Successfully finished processing state updates",
          Level.INFO -> "Stopped Indexer Server",
        )
        testIndexer.openSubscriptions shouldBe mutable.Set.empty
      }
    }

    "wait until the subscription completes" in {
      val recoveringIndexer = new RecoveringIndexer(actorSystem.scheduler, 10.millis)
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SuccessfullyCompletes, 100.millis, 10.millis),
      )

      val resource = recoveringIndexer.start(() => testIndexer.subscribe())
      resource.asFuture
        .map { complete =>
          readLog() should contain theSameElementsInOrderAs Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
          )
          complete
        }
        .flatten
        .transformWith(finallyRelease(resource))
        .map { _ =>
          readLog() should contain theSameElementsInOrderAs Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
            Level.INFO -> "Successfully finished processing state updates",
            Level.INFO -> "Stopping Indexer Server",
            Level.INFO -> "Stopped Indexer Server",
          )
          testIndexer.openSubscriptions shouldBe mutable.Set.empty
        }
    }

    "recover from failure" in {
      val recoveringIndexer = new RecoveringIndexer(actorSystem.scheduler, 10.millis)
      // Subscribe fails, then the stream fails, then the stream completes without errors.
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SubscriptionFails, 10.millis, 10.millis),
        SubscribeResult("B", StreamFails, 10.millis, 10.millis),
        SubscribeResult("C", SuccessfullyCompletes, 10.millis, 10.millis),
      )

      val resource = recoveringIndexer.start(() => testIndexer.subscribe())

      resource.asFuture.flatten
        .transformWith(finallyRelease(resource))
        .map { _ =>
          testIndexer.actions shouldBe Seq[IndexerEvent](
            EventSubscribeCalled("A"),
            EventSubscribeFail("A"),
            EventSubscribeCalled("B"),
            EventSubscribeSuccess("B"),
            EventStreamFail("B"),
            EventStopCalled("B"),
            EventSubscribeCalled("C"),
            EventSubscribeSuccess("C"),
            EventStreamComplete("C"),
            EventStopCalled("C"),
          )
          readLog() should contain theSameElementsInOrderAs Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.ERROR -> "Error while starting indexer, restart scheduled after 10 milliseconds",
            Level.INFO -> "Restarting Indexer Server",
            Level.INFO -> "Restarted Indexer Server",
            Level.ERROR -> "Error while running indexer, restart scheduled after 10 milliseconds",
            Level.INFO -> "Restarting Indexer Server",
            Level.INFO -> "Restarted Indexer Server",
            Level.INFO -> "Successfully finished processing state updates",
            Level.INFO -> "Stopping Indexer Server",
            Level.INFO -> "Stopped Indexer Server",
          )
          testIndexer.openSubscriptions shouldBe mutable.Set.empty
        }
    }

    "respect restart delay" in {
      val restartDelay = 500.millis
      val recoveringIndexer = new RecoveringIndexer(actorSystem.scheduler, restartDelay)
      // Subscribe fails, then the stream completes without errors. Note the restart delay of 500ms.
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SubscriptionFails, 0.millis, 0.millis),
        SubscribeResult("B", SuccessfullyCompletes, 0.millis, 0.millis),
      )

      val t0 = System.nanoTime()
      val resource = recoveringIndexer.start(() => testIndexer.subscribe())
      resource.asFuture.flatten
        .transformWith(finallyRelease(resource))
        .map { _ =>
          val t1 = System.nanoTime()
          (t1 - t0).nanos should be >= restartDelay
          testIndexer.actions shouldBe Seq[IndexerEvent](
            EventSubscribeCalled("A"),
            EventSubscribeFail("A"),
            EventSubscribeCalled("B"),
            EventSubscribeSuccess("B"),
            EventStreamComplete("B"),
            EventStopCalled("B"),
          )
          readLog() should contain theSameElementsInOrderAs Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.ERROR -> "Error while starting indexer, restart scheduled after 500 milliseconds",
            Level.INFO -> "Restarting Indexer Server",
            Level.INFO -> "Restarted Indexer Server",
            Level.INFO -> "Successfully finished processing state updates",
            Level.INFO -> "Stopping Indexer Server",
            Level.INFO -> "Stopped Indexer Server",
          )
          testIndexer.openSubscriptions shouldBe mutable.Set.empty
        }
    }
  }

}

object RecoveringIndexerSpec {

  def finallyRelease[T](resource: Resource[_])(
      implicit executionContext: ExecutionContext
  ): Try[T] => Future[T] = {
    case Success(value) => resource.release().map(_ => value)
    case Failure(exception) => resource.release().flatMap(_ => Future.failed(exception))
  }

  class TestIndexer(resultsSeq: SubscribeResult*) {
    private[this] val actorSystem = ActorSystem("TestIndexer")
    private[this] val scheduler = actorSystem.scheduler

    private[this] val results = resultsSeq.iterator
    private[this] val actionsQueue = new ConcurrentLinkedQueue[IndexerEvent]()

    val openSubscriptions: mutable.Set[IndexFeedHandle] = mutable.Set()

    def actions: Seq[IndexerEvent] = actionsQueue.toArray(Array.empty[IndexerEvent]).toSeq

    class TestIndexerFeedHandle(result: SubscribeResult)(
        implicit executionContext: ExecutionContext
    ) extends IndexFeedHandle {
      private[this] val promise = Promise[Unit]()

      if (result.status == SuccessfullyCompletes) {
        scheduler.scheduleOnce(result.completeDelay)({
          actionsQueue.add(EventStreamComplete(result.name))
          promise.trySuccess(())
          ()
        })
      } else {
        scheduler.scheduleOnce(result.completeDelay)({
          actionsQueue.add(EventStreamFail(result.name))
          promise.tryFailure(new RuntimeException("Random simulated failure: subscribe"))
          ()
        })
      }

      def stop(): Future[Unit] = {
        actionsQueue.add(EventStopCalled(result.name))
        promise.trySuccess(())
        promise.future
      }

      override def completed(): Future[Unit] = {
        promise.future
      }
    }

    def subscribe()(implicit context: ResourceContext): Resource[IndexFeedHandle] =
      new Subscription().acquire()

    class Subscription extends ResourceOwner[IndexFeedHandle] {
      override def acquire()(implicit context: ResourceContext): Resource[IndexFeedHandle] = {
        val result = results.next()
        Resource(Future {
          actionsQueue.add(EventSubscribeCalled(result.name))
        }.flatMap { _ =>
          after(result.subscribeDelay, scheduler)(Future {
            if (result.status != SubscriptionFails) {
              actionsQueue.add(EventSubscribeSuccess(result.name))
              val handle = new TestIndexerFeedHandle(result)
              openSubscriptions += handle
              handle
            } else {
              actionsQueue.add(EventSubscribeFail(result.name))
              throw new RuntimeException("Random simulated failure: subscribe")
            }
          })
        })(
          handle => {
            val complete = handle.stop()
            complete.onComplete { _ =>
              openSubscriptions -= handle
            }
            complete
          }
        )
      }
    }

  }

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
