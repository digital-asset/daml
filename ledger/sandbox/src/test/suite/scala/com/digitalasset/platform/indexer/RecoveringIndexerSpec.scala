// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorSystem
import akka.pattern.after
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.indexer.RecoveringIndexerSpec._
import com.digitalasset.resources.{Resource, ResourceOwner}
import org.mockito.ArgumentMatchersSugar._
import org.mockito.Mockito
import org.mockito.MockitoSugar._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, BeforeAndAfterEach, Matchers}
import org.slf4j.Logger

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

class RecoveringIndexerSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with BeforeAndAfterEach {

  private[this] implicit val executionContext: ExecutionContext = DirectExecutionContext
  private[this] var actorSystem: ActorSystem = _

  private[this] var loggerFactory: NamedLoggerFactory = _
  private[this] var logger: Logger = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    actorSystem = ActorSystem(getClass.getSimpleName)

    loggerFactory = mock[NamedLoggerFactory]
    logger = mock[Logger]
    when(loggerFactory.getLogger(classOf[RecoveringIndexer])).thenReturn(logger)
    ()
  }

  override def afterEach(): Unit = {
    Await.result(actorSystem.terminate(), 10.seconds)
    verifyNoMoreInteractions(logger)
    super.afterEach()
  }

  "RecoveringIndexer" should {
    "work when the stream completes" in {
      val recoveringIndexer =
        new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second, loggerFactory)
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
          val inOrder = Mockito.inOrder(logger)
          inOrder.verify(logger).info("Starting Indexer Server")
          inOrder.verify(logger).info("Started Indexer Server")
          inOrder.verify(logger).info("Successfully finished processing state updates")
          inOrder.verify(logger).info("Stopping Indexer Server")
          inOrder.verify(logger).info("Stopped Indexer Server")
          testIndexer.openSubscriptions shouldBe mutable.Set.empty
        }
    }

    "work when the stream is stopped" in {
      val recoveringIndexer =
        new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second, loggerFactory)
      // Stream completes after 10s, but is released before that happens
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SuccessfullyCompletes, 10.millis, 10.seconds),
      )

      val resource = recoveringIndexer.start(() => testIndexer.subscribe())

      for {
        _ <- akka.pattern.after(100.millis, actorSystem.scheduler)(Future.successful(()))
        _ <- resource.release()
        complete <- resource.asFuture
        _ <- complete
      } yield {
        testIndexer.actions shouldBe Seq[IndexerEvent](
          EventSubscribeCalled("A"),
          EventSubscribeSuccess("A"),
          EventStopCalled("A"),
        )
        val inOrder = Mockito.inOrder(logger)
        inOrder.verify(logger).info("Starting Indexer Server")
        inOrder.verify(logger).info("Started Indexer Server")
        inOrder.verify(logger).info("Stopping Indexer Server")
        inOrder.verify(logger).info("Successfully finished processing state updates")
        inOrder.verify(logger).info("Stopped Indexer Server")
        testIndexer.openSubscriptions shouldBe mutable.Set.empty
      }
    }

    "wait until the subscription completes" in {
      val recoveringIndexer =
        new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second, loggerFactory)
      val testIndexer = new TestIndexer(
        SubscribeResult("A", SuccessfullyCompletes, 100.millis, 10.millis),
      )

      val resource = recoveringIndexer.start(() => testIndexer.subscribe())
      resource.asFuture
        .map { complete =>
          val inOrder = Mockito.inOrder(logger)
          inOrder.verify(logger).info("Starting Indexer Server")
          inOrder.verify(logger).info("Started Indexer Server")
          complete
        }
        .flatten
        .transformWith(finallyRelease(resource))
        .map { _ =>
          val inOrder = Mockito.inOrder(logger)
          inOrder.verify(logger).info("Starting Indexer Server")
          inOrder.verify(logger).info("Started Indexer Server")
          inOrder.verify(logger).info("Successfully finished processing state updates")
          inOrder.verify(logger).info("Stopping Indexer Server")
          inOrder.verify(logger).info("Stopped Indexer Server")
          testIndexer.openSubscriptions shouldBe mutable.Set.empty
        }
    }

    "recover from failure" in {
      val recoveringIndexer =
        new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second, loggerFactory)
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
          val inOrder = Mockito.inOrder(logger)
          inOrder.verify(logger).info("Starting Indexer Server")
          inOrder
            .verify(logger)
            .error(
              eqTo("Error while starting indexer, restart scheduled after 10 milliseconds"),
              isA[Throwable])
          inOrder.verify(logger).info("Restarting Indexer Server")
          inOrder.verify(logger).info("Restarted Indexer Server")
          inOrder
            .verify(logger)
            .error(
              eqTo("Error while running indexer, restart scheduled after 10 milliseconds"),
              isA[Throwable])
          inOrder.verify(logger).info("Restarting Indexer Server")
          inOrder.verify(logger).info("Restarted Indexer Server")
          inOrder.verify(logger).info("Successfully finished processing state updates")
          inOrder.verify(logger).info("Stopping Indexer Server")
          inOrder.verify(logger).info("Stopped Indexer Server")
          testIndexer.openSubscriptions shouldBe mutable.Set.empty
        }
    }

    "respect restart delay" in {
      val recoveringIndexer =
        new RecoveringIndexer(actorSystem.scheduler, 500.millis, 1.second, loggerFactory)
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
          (t1 - t0) should be >= 500.millis.toNanos
          testIndexer.actions shouldBe Seq[IndexerEvent](
            EventSubscribeCalled("A"),
            EventSubscribeFail("A"),
            EventSubscribeCalled("B"),
            EventSubscribeSuccess("B"),
            EventStreamComplete("B"),
            EventStopCalled("B"),
          )
          val inOrder = Mockito.inOrder(logger)
          inOrder.verify(logger).info("Starting Indexer Server")
          inOrder
            .verify(logger)
            .error(
              eqTo("Error while starting indexer, restart scheduled after 500 milliseconds"),
              isA[Throwable])
          inOrder.verify(logger).info("Restarting Indexer Server")
          inOrder.verify(logger).info("Restarted Indexer Server")
          inOrder.verify(logger).info("Successfully finished processing state updates")
          inOrder.verify(logger).info("Stopping Indexer Server")
          inOrder.verify(logger).info("Stopped Indexer Server")
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

    def subscribe()(implicit executionContext: ExecutionContext): Resource[IndexFeedHandle] =
      new Subscription().acquire()

    class Subscription extends ResourceOwner[IndexFeedHandle] {
      override def acquire()(
          implicit executionContext: ExecutionContext
      ): Resource[IndexFeedHandle] = {
        val result = results.next()
        Resource[TestIndexerFeedHandle](
          Future {
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
          },
          handle => {
            val complete = handle.stop()
            complete.onComplete { _ =>
              openSubscriptions -= handle
            }
            complete
          }
        ).vary
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
