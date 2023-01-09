// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import akka.pattern.after
import ch.qos.logback.classic.Level
import com.daml.ledger.api.health.{HealthStatus, Healthy, Unhealthy}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner, TestResourceContext}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.indexer.RecoveringIndexerSpec._
import com.daml.platform.testing.LogCollector
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.control.NoStackTrace
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
      val timeout = 10.millis
      val recoveringIndexer =
        RecoveringIndexer(actorSystem.scheduler, actorSystem.dispatcher, timeout)
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SuccessfullyCompletes, timeout, timeout)
      )

      val resource = recoveringIndexer.start(testIndexer())
      for {
        (healthReporter, complete) <- resource.asFuture
        _ <- complete.transformWith(finallyRelease(resource))
      } yield {
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
        // When the indexer is shutdown, its status should be unhealthy/not serving
        healthReporter.currentHealth() shouldBe Unhealthy
      }
    }

    "work when the stream is stopped" in {
      val timeout = 10.millis
      val recoveringIndexer =
        RecoveringIndexer(actorSystem.scheduler, actorSystem.dispatcher, timeout)
      // Stream completes after 10s, but is released before that happens
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SuccessfullyCompletes, timeout, 10.seconds)
      )

      val resource = recoveringIndexer.start(testIndexer())

      for {
        _ <- akka.pattern.after(100.millis, actorSystem.scheduler)(Future.unit)
        _ <- resource.release()
        (healthReporter, complete) <- resource.asFuture
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
        // When the indexer is shutdown, its status should be unhealthy/not serving
        healthReporter.currentHealth() shouldBe Unhealthy
      }
    }

    "wait until the subscription completes" in {
      val timeout = 10.millis
      val recoveringIndexer =
        RecoveringIndexer(actorSystem.scheduler, actorSystem.dispatcher, timeout)
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SuccessfullyCompletes, 100.millis, timeout)
      )

      val resource = recoveringIndexer.start(testIndexer())
      for {
        (healthReporter, complete) <- resource.asFuture
        _ <- Future {
          readLog().take(2) should contain theSameElementsInOrderAs Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
          )
        }
        _ <- complete.transformWith(finallyRelease(resource))
      } yield {
        readLog() should contain theSameElementsInOrderAs Seq(
          Level.INFO -> "Starting Indexer Server",
          Level.INFO -> "Started Indexer Server",
          Level.INFO -> "Successfully finished processing state updates",
          Level.INFO -> "Stopping Indexer Server",
          Level.INFO -> "Stopped Indexer Server",
        )
        testIndexer.openSubscriptions shouldBe mutable.Set.empty
        healthReporter.currentHealth() shouldBe Unhealthy
      }
    }

    "recover from failure" in {
      val timeout = 10.millis
      val recoveringIndexer =
        RecoveringIndexer(actorSystem.scheduler, actorSystem.dispatcher, timeout)
      // Subscribe fails, then the stream fails, then the stream completes without errors.
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SubscriptionFails, timeout, timeout),
        SubscribeResult("B", StreamFails, timeout, timeout),
        SubscribeResult("C", SuccessfullyCompletes, timeout, timeout),
      )

      val resource = recoveringIndexer.start(testIndexer())

      for {
        (_, complete) <- resource.asFuture
        _ <- complete.transformWith(finallyRelease(resource))
      } yield {
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

    "report correct health status updates on failures and recoveries" in {
      val logger = ContextualizedLogger.get(getClass)
      val healthStatusLogCapture = mutable.ArrayBuffer.empty[HealthStatus]
      val healthStatusRef = new AtomicReference[HealthStatus]()

      val timeout = 100.millis
      val recoveringIndexer = new RecoveringIndexer(
        actorSystem.scheduler,
        actorSystem.dispatcher,
        timeout,
        updateHealthStatus = healthStatus => {
          logger.info(s"Updating the health status of the indexer to $healthStatus.")
          healthStatusLogCapture += healthStatus
          healthStatusRef.set(healthStatus)
        },
        () => healthStatusRef.get(),
      )
      // Subscribe fails, then the stream fails, then the stream completes without errors.
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SubscriptionFails, timeout, timeout),
        SubscribeResult("B", StreamFails, timeout, timeout),
        SubscribeResult("C", SuccessfullyCompletes, timeout, timeout),
      )

      val resource = recoveringIndexer.start(testIndexer())

      for {
        (healthReporter, complete) <- resource.asFuture
        _ <- complete.transformWith(finallyRelease(resource))
      } yield {
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

        testIndexer.openSubscriptions shouldBe mutable.Set.empty

        healthStatusLogCapture should contain theSameElementsInOrderAs Seq(
          Unhealthy,
          Healthy,
          Unhealthy,
          Healthy,
          Unhealthy,
        )
        // When the indexer is shutdown, its status should be unhealthy/not serving
        healthReporter.currentHealth() shouldBe Unhealthy
      }
    }

    "respect restart delay" in {
      val restartDelay = 500.millis
      val recoveringIndexer =
        RecoveringIndexer(actorSystem.scheduler, actorSystem.dispatcher, restartDelay)
      // Subscribe fails, then the stream completes without errors. Note the restart delay of 500ms.
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SubscriptionFails, 0.millis, 0.millis),
        SubscribeResult("B", SuccessfullyCompletes, 0.millis, 0.millis),
      )

      val t0 = System.nanoTime()
      val resource = recoveringIndexer.start(testIndexer())
      resource.asFuture
        .flatMap(_._2)
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
          // All log items will be in order except this one, which could show up anywhere.
          // It could finish restarting after stopping, for example.
          val sequentialLog = readLog().filterNot(_ == Level.INFO -> "Restarted Indexer Server")
          sequentialLog should contain theSameElementsInOrderAs Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.ERROR -> "Error while starting indexer, restart scheduled after 500 milliseconds",
            Level.INFO -> "Restarting Indexer Server",
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

  def finallyRelease[T](resource: Resource[_])(implicit
      executionContext: ExecutionContext
  ): Try[T] => Future[T] = {
    case Success(value) => resource.release().map(_ => value)
    case Failure(exception) => resource.release().flatMap(_ => Future.failed(exception))
  }

  final class TestIndexer(resultsSeq: SubscribeResult*)(implicit loggingContext: LoggingContext) {
    private[this] val logger = ContextualizedLogger.get(getClass)
    private[this] val actorSystem = ActorSystem("TestIndexer")
    private[this] val scheduler = actorSystem.scheduler

    private[this] val results = resultsSeq.iterator
    private[this] val actionsQueue = new ConcurrentLinkedQueue[IndexerEvent]()

    private def addAction(event: IndexerEvent): Unit = {
      logger.info(s"New test event: $event")
      actionsQueue.add(event)
      ()
    }

    val openSubscriptions: mutable.Set[Future[Unit]] = mutable.Set()

    def actions: Seq[IndexerEvent] = actionsQueue.toArray(Array.empty[IndexerEvent]).toSeq

    class TestIndexerFeedHandle(
        result: SubscribeResult
    )(implicit executionContext: ExecutionContext) {
      private[this] val promise = Promise[Unit]()

      if (result.status == SuccessfullyCompletes) {
        scheduler.scheduleOnce(result.completeDelay)({
          addAction(EventStreamComplete(result.name))
          promise.trySuccess(())
          ()
        })
      } else {
        scheduler.scheduleOnce(result.completeDelay)({
          addAction(EventStreamFail(result.name))
          promise.tryFailure(new TestIndexerException("stream"))
          ()
        })
      }

      def stop(): Future[Unit] = {
        addAction(EventStopCalled(result.name))
        promise.trySuccess(())
        promise.future
      }

      def completed(): Future[Unit] = {
        promise.future
      }
    }

    def apply(): Indexer = new Subscription()

    class Subscription extends ResourceOwner[Future[Unit]] {
      override def acquire()(implicit context: ResourceContext): Resource[Future[Unit]] = {
        val result = results.next()
        Resource(Future {
          addAction(EventSubscribeCalled(result.name))
        }.flatMap { _ =>
          after(result.subscribeDelay, scheduler)(Future {
            if (result.status != SubscriptionFails) {
              addAction(EventSubscribeSuccess(result.name))
              val handle = new TestIndexerFeedHandle(result)
              openSubscriptions += handle.completed()
              handle
            } else {
              addAction(EventSubscribeFail(result.name))
              throw new TestIndexerException("subscribe")
            }
          })
        })(handle => {
          handle.stop().transform { complete =>
            openSubscriptions -= handle.completed()
            complete
          }
        }).map(_.completed())
      }
    }
  }

  final class TestIndexerException(location: String)
      extends RuntimeException(s"Simulated failure: $location")
      with NoStackTrace

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
