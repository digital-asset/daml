// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner, TestResourceContext}
import com.digitalasset.canton.ledger.api.health.{HealthStatus, Healthy, Unhealthy}
import com.digitalasset.canton.logging.{
  NamedLoggerFactory,
  NamedLogging,
  SuppressingLogger,
  SuppressionRule,
}
import com.digitalasset.canton.platform.indexer.RecoveringIndexerSpec.*
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.after
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfterEach, Succeeded}
import org.slf4j.event.Level

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

final class RecoveringIndexerSpec
    extends AsyncWordSpec
    with Matchers
    with TestResourceContext
    with BeforeAndAfterEach
    with Eventually
    with IntegrationPatience
    with NamedLogging {

  private[this] implicit def traceContext: TraceContext = TraceContext.empty
  private[this] var actorSystem: ActorSystem = _
  override val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)

  override def beforeEach(): Unit = {
    super.beforeEach()
    actorSystem = ActorSystem(getClass.getSimpleName)
  }

  override def afterEach(): Unit = {
    Await.result(actorSystem.terminate(), 10.seconds)
    super.afterEach()
  }

  val RecoveringIndexerSuppressionRule: SuppressionRule =
    SuppressionRule.forLogger[RecoveringIndexer] && !SuppressionRule.forLogger[TestIndexer]

  "RecoveringIndexer" should {
    "work when the stream completes" in {
      loggerFactory.suppress(RecoveringIndexerSuppressionRule) {
        val timeout = 10.millis
        val recoveringIndexer =
          RecoveringIndexer(actorSystem.scheduler, actorSystem.dispatcher, timeout, loggerFactory)
        val testIndexer = new TestIndexer(
          loggerFactory,
          SubscribeResult("A", SuccessfullyCompletes, timeout, timeout),
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
          assertLogsInOrder(
            Seq(
              Level.INFO -> "Starting Indexer Server",
              Level.INFO -> "Started Indexer Server",
              Level.INFO -> "Successfully finished processing state updates",
              Level.INFO -> "Stopping Indexer Server",
              Level.INFO -> "Stopped Indexer Server",
            )
          )
          testIndexer.openSubscriptions shouldBe mutable.Set.empty
          // When the indexer is shutdown, its status should be unhealthy/not serving
          healthReporter.currentHealth() shouldBe Unhealthy
        }
      }
    }

    "work when the stream is stopped" in {
      loggerFactory.suppress(RecoveringIndexerSuppressionRule) {
        val timeout = 10.millis
        val recoveringIndexer =
          RecoveringIndexer(actorSystem.scheduler, actorSystem.dispatcher, timeout, loggerFactory)
        // Stream completes after 10s, but is released before that happens
        val testIndexer = new TestIndexer(
          loggerFactory,
          SubscribeResult("A", SuccessfullyCompletes, timeout, 10.seconds),
        )

        val resource = recoveringIndexer.start(testIndexer())

        for {
          _ <- org.apache.pekko.pattern.after(100.millis, actorSystem.scheduler)(Future.unit)
          _ <- resource.release()
          (healthReporter, complete) <- resource.asFuture
          _ <- complete
        } yield {
          testIndexer.actions shouldBe Seq[IndexerEvent](
            EventSubscribeCalled("A"),
            EventSubscribeSuccess("A"),
            EventStopCalled("A"),
          )
          assertLogsInOrder(
            Seq(
              Level.INFO -> "Starting Indexer Server",
              Level.INFO -> "Started Indexer Server",
              Level.INFO -> "Stopping Indexer Server",
              Level.INFO -> "Successfully finished processing state updates",
              Level.INFO -> "Stopped Indexer Server",
            )
          )
          testIndexer.openSubscriptions shouldBe mutable.Set.empty
          // When the indexer is shutdown, its status should be unhealthy/not serving
          healthReporter.currentHealth() shouldBe Unhealthy
        }
      }
    }

    "wait until the subscription completes" in {
      loggerFactory.suppress(RecoveringIndexerSuppressionRule) {
        val timeout = 10.millis
        val recoveringIndexer =
          RecoveringIndexer(actorSystem.scheduler, actorSystem.dispatcher, timeout, loggerFactory)
        val testIndexer = new TestIndexer(
          loggerFactory,
          SubscribeResult("A", SuccessfullyCompletes, 100.millis, timeout),
        )

        val resource = recoveringIndexer.start(testIndexer())
        for {
          (healthReporter, complete) <- resource.asFuture
          _ <- Future {
            eventually(
              loggerFactory.fetchRecordedLogEntries
                .map(entry => entry.level -> entry.message)
                .take(2) should contain theSameElementsInOrderAs Seq(
                Level.INFO -> "Starting Indexer Server",
                Level.INFO -> "Started Indexer Server",
              )
            )
          }
          _ <- complete.transformWith(finallyRelease(resource))
        } yield {
          assertLogsInOrder(
            Seq(
              Level.INFO -> "Starting Indexer Server",
              Level.INFO -> "Started Indexer Server",
              Level.INFO -> "Successfully finished processing state updates",
              Level.INFO -> "Stopping Indexer Server",
              Level.INFO -> "Stopped Indexer Server",
            )
          )
          testIndexer.openSubscriptions shouldBe mutable.Set.empty
          healthReporter.currentHealth() shouldBe Unhealthy
        }
      }
    }

    "recover from failure" in {
      loggerFactory.suppress(RecoveringIndexerSuppressionRule) {
        val timeout = 10.millis
        val recoveringIndexer =
          RecoveringIndexer(actorSystem.scheduler, actorSystem.dispatcher, timeout, loggerFactory)
        // Subscribe fails, then the stream fails, then the stream completes without errors.
        val testIndexer = new TestIndexer(
          loggerFactory,
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
          assertLogsInOrder(
            Seq(
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
          )
          testIndexer.openSubscriptions shouldBe mutable.Set.empty
        }
      }
    }

    "report correct health status updates on failures and recoveries" in {
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
        loggerFactory,
      )
      // Subscribe fails, then the stream fails, then the stream completes without errors.
      val testIndexer = new TestIndexer(
        loggerFactory,
        SubscribeResult("A", SubscriptionFails, timeout, timeout),
        SubscribeResult("B", StreamFails, timeout, timeout),
        SubscribeResult("C", SuccessfullyCompletes, timeout, timeout),
      )

      val resource = recoveringIndexer.start(testIndexer())
      loggerFactory.suppress(RecoveringIndexerSuppressionRule) {

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
    }

    "respect restart delay" in {
      loggerFactory.suppress(RecoveringIndexerSuppressionRule) {
        val restartDelay = 500.millis
        val recoveringIndexer =
          RecoveringIndexer(
            actorSystem.scheduler,
            actorSystem.dispatcher,
            restartDelay,
            loggerFactory,
          )
        // Subscribe fails, then the stream completes without errors. Note the restart delay of 500ms.
        val testIndexer = new TestIndexer(
          loggerFactory,
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
            loggerFactory.fetchRecordedLogEntries
              .map(entry => entry.level -> entry.message)
              .filterNot(
                _ == Level.INFO -> "Restarted Indexer Server"
              ) should contain theSameElementsInOrderAs Seq(
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

  private def assertLogsInOrder(expected: Seq[(Level, String)]): Assertion = {
    loggerFactory.fetchRecordedLogEntries
      .map(entry => entry.level -> entry.message) should contain theSameElementsInOrderAs expected
    Succeeded
  }
}

object RecoveringIndexerSpec {

  def finallyRelease[T](resource: Resource[_])(implicit
      executionContext: ExecutionContext
  ): Try[T] => Future[T] = {
    case Success(value) => resource.release().map(_ => value)
    case Failure(exception) => resource.release().flatMap(_ => Future.failed(exception))
  }

  final class TestIndexer(val loggerFactory: NamedLoggerFactory, resultsSeq: SubscribeResult*)(
      implicit traceContext: TraceContext
  ) extends NamedLogging {
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

  final case class SubscribeResult(
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
