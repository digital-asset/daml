// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorSystem
import akka.pattern.after
import ch.qos.logback.classic.Level
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.platform.indexer.RecoveringIndexerSpec._
import com.digitalasset.platform.indexer.TestIndexer._
import com.digitalasset.platform.sandbox.logging.TestNamedLoggerFactory
import com.digitalasset.resources.{Resource, ResourceOwner}
import org.scalatest.{AsyncWordSpec, BeforeAndAfterEach, Matchers}

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

class TestIndexer(results: Iterator[SubscribeResult]) {
  private[this] val actorSystem = ActorSystem("TestIndexer")
  private[this] val scheduler = actorSystem.scheduler

  val actions = new ConcurrentLinkedQueue[IndexerEvent]()

  val openSubscriptions: mutable.Set[IndexFeedHandle] = mutable.Set()

  class TestIndexerFeedHandle(result: SubscribeResult)(implicit executionContext: ExecutionContext)
      extends IndexFeedHandle {
    private[this] val promise = Promise[Unit]()

    if (result.status == SuccessfullyCompletes) {
      scheduler.scheduleOnce(result.completeDelay)({
        actions.add(EventStreamComplete(result.name))
        promise.trySuccess(())
        ()
      })
    } else {
      scheduler.scheduleOnce(result.completeDelay)({
        actions.add(EventStreamFail(result.name))
        promise.tryFailure(new RuntimeException("Random simulated failure: subscribe"))
        ()
      })
    }

    def stop(): Future[Unit] = {
      actions.add(EventStopCalled(result.name))
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
          actions.add(EventSubscribeCalled(result.name))
        }.flatMap { _ =>
          if (result.status != SubscriptionFails) {
            after(result.subscribeDelay, scheduler)(Future {
              actions.add(EventSubscribeSuccess(result.name))
              val handle = new TestIndexerFeedHandle(result)
              openSubscriptions += handle
              handle
            })
          } else {
            after(result.subscribeDelay, scheduler)(Future {
              actions.add(EventSubscribeFail(result.name))
              throw new RuntimeException("Random simulated failure: subscribe")
            })
          }
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

      val resource = recoveringIndexer.start(() => testIndexer.subscribe())
      resource.asFuture.flatten
        .transformWith(finallyRelease(resource))
        .map { _ =>
          List(testIndexer.actions.toArray: _*) should contain theSameElementsInOrderAs List[
            IndexerEvent](
            EventSubscribeCalled("A"),
            EventSubscribeSuccess("A"),
            EventStreamComplete("A"),
            EventStopCalled("A"),
          )
          logs should be(
            Seq(
              Level.INFO -> "Starting Indexer Server",
              Level.INFO -> "Started Indexer Server",
              Level.INFO -> "Successfully finished processing state updates",
            ))
          testIndexer.openSubscriptions should be(mutable.Set.empty)
        }
    }

    "work when the stream is stopped" in {
      val recoveringIndexer =
        new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second, loggerFactory)
      // Stream completes after 10s, but is released before that happens
      val testIndexer = new TestIndexer(
        List(
          SubscribeResult("A", SuccessfullyCompletes, 10.millis, 10.seconds),
        ).iterator)

      val resource = recoveringIndexer.start(() => testIndexer.subscribe())

      val done = for {
        _ <- akka.pattern.after(100.millis, scheduler)(Future.successful(()))
        _ <- resource.release()
        complete <- resource.asFuture
        _ <- complete
      } yield ()

      done
        .map { _ =>
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
          testIndexer.openSubscriptions should be(mutable.Set.empty)
        }
    }

    "wait until the subscription completes" in {
      val recoveringIndexer =
        new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second, loggerFactory)
      val testIndexer = new TestIndexer(
        List(
          SubscribeResult("A", SuccessfullyCompletes, 100.millis, 10.millis)
        ).iterator)

      val resource = recoveringIndexer.start(() => testIndexer.subscribe())
      resource.asFuture
        .map { complete =>
          logs should be(
            Seq(
              Level.INFO -> "Starting Indexer Server",
              Level.INFO -> "Started Indexer Server",
            ))
          complete
        }
        .flatten
        .transformWith(finallyRelease(resource))
        .map { _ =>
          logs should be(
            Seq(
              Level.INFO -> "Starting Indexer Server",
              Level.INFO -> "Started Indexer Server",
              Level.INFO -> "Successfully finished processing state updates",
            ))
          testIndexer.openSubscriptions should be(mutable.Set.empty)
        }
    }

    "recover from failure" in {
      val recoveringIndexer =
        new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second, loggerFactory)
      // Subscribe fails, then the stream fails, then the stream completes without errors.
      val testIndexer = new TestIndexer(
        List(
          SubscribeResult("A", SubscriptionFails, 10.millis, 10.millis),
          SubscribeResult("B", StreamFails, 10.millis, 10.millis),
          SubscribeResult("C", SuccessfullyCompletes, 10.millis, 10.millis),
        ).iterator)

      val resource = recoveringIndexer.start(() => testIndexer.subscribe())

      resource.asFuture.flatten
        .transformWith(finallyRelease(resource))
        .map { _ =>
          List(testIndexer.actions.toArray: _*) should contain theSameElementsInOrderAs List[
            IndexerEvent](
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
          logs should be(Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.ERROR -> "Error while running indexer, restart scheduled after 10 milliseconds",
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
            Level.ERROR -> "Error while running indexer, restart scheduled after 10 milliseconds",
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
            Level.INFO -> "Successfully finished processing state updates",
          ))
          testIndexer.openSubscriptions should be(mutable.Set.empty)
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
      val resource = recoveringIndexer.start(() => testIndexer.subscribe())
      resource.asFuture.flatten
        .transformWith(finallyRelease(resource))
        .map { _ =>
          val t1 = System.nanoTime()
          (t1 - t0) should be >= 500.millis.toNanos
          List(testIndexer.actions.toArray: _*) should contain theSameElementsInOrderAs List[
            IndexerEvent](
            EventSubscribeCalled("A"),
            EventSubscribeFail("A"),
            EventSubscribeCalled("B"),
            EventSubscribeSuccess("B"),
            EventStreamComplete("B"),
            EventStopCalled("B"),
          )
          logs should be(Seq(
            Level.INFO -> "Starting Indexer Server",
            Level.ERROR -> "Error while running indexer, restart scheduled after 500 milliseconds",
            Level.INFO -> "Starting Indexer Server",
            Level.INFO -> "Started Indexer Server",
            Level.INFO -> "Successfully finished processing state updates",
          ))
          testIndexer.openSubscriptions should be(mutable.Set.empty)
        }
    }
  }

  private def logs: Seq[TestNamedLoggerFactory.LogEvent] =
    loggerFactory.logs(classOf[RecoveringIndexer])
}

object RecoveringIndexerSpec {
  def finallyRelease[T](resource: Resource[_])(
      implicit executionContext: ExecutionContext
  ): Try[T] => Future[T] = {
    case Success(value) => resource.release().map(_ => value)
    case Failure(exception) => resource.release().flatMap(_ => Future.failed(exception))
  }
}
