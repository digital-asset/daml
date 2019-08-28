// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorSystem
import akka.pattern.after
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._

case class SubscribeResult(
    name: String,
    subscribeDelay: FiniteDuration,
    subscribeSucceeds: Boolean,
    completeDelay: FiniteDuration,
    completeSucceeds: Boolean)

sealed abstract class IndexerEvent {
  def name: String
}
final case class EventStreamFail(name: String) extends IndexerEvent
final case class EventStreamComplete(name: String) extends IndexerEvent
final case class EventStopCalled(name: String) extends IndexerEvent
final case class EventSubscribeCalled(name: String) extends IndexerEvent
final case class EventSubscribeSuccess(name: String) extends IndexerEvent
final case class EventSubscribeFail(name: String) extends IndexerEvent

class TestIndexer(results: Iterator[SubscribeResult]) {
  private[this] val actorSystem = ActorSystem("TestIndexer")
  private[this] val scheduler = actorSystem.scheduler

  val actions = new ConcurrentLinkedQueue[IndexerEvent]()

  class TestIndexerFeedHandle(result: SubscribeResult) extends IndexFeedHandle {
    private[this] val promise = Promise[akka.Done]()

    if (result.completeSucceeds) {
      scheduler.scheduleOnce(result.completeDelay)({
        actions.add(EventStreamComplete(result.name))
        promise.trySuccess(akka.Done)
        ()
      })(DEC)
    } else {
      scheduler.scheduleOnce(result.completeDelay)({
        actions.add(EventStreamFail(result.name))
        promise.tryFailure(new RuntimeException("Random simulated failure: subscribe"))
        ()
      })(DEC)
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

  def subscribe(): Future[IndexFeedHandle] = {
    val result = results.next()
    actions.add(EventSubscribeCalled(result.name))
    if (result.subscribeSucceeds) {
      after(result.subscribeDelay, scheduler)({
        actions.add(EventSubscribeSuccess(result.name))
        Future.successful(new TestIndexerFeedHandle(result))
      })(DEC)
    } else {
      after(result.subscribeDelay, scheduler)({
        actions.add(EventSubscribeFail(result.name))
        Future.failed(new RuntimeException("Random simulated failure: subscribe"))
      })(DEC)
    }
  }
}

class RecoveringIndexerIT extends AsyncWordSpec with Matchers {

  private[this] implicit val ec: ExecutionContext = DEC
  private[this] val actorSystem = ActorSystem("RecoveringIndexerIT")
  private[this] val scheduler = actorSystem.scheduler

  "RecoveringIndexer" should {

    "work when the stream completes" in {
      val recoveringIndexer = new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second)
      val testIndexer = new TestIndexer(
        List(
          SubscribeResult("A", 10.millis, true, 10.millis, true)
        ).iterator)

      val end = recoveringIndexer.start(() => testIndexer.subscribe())
      end map { _ =>
        List(testIndexer.actions.toArray: _*) should contain theSameElementsInOrderAs List[
          IndexerEvent](
          EventSubscribeCalled("A"),
          EventSubscribeSuccess("A"),
          EventStreamComplete("A")
        )
      }
    }

    "work when the stream is stopped" in {
      val recoveringIndexer = new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second)
      // Stream completes after 10sec, but stop() is called before
      val testIndexer = new TestIndexer(
        List(
          SubscribeResult("A", 10.millis, true, 10000.millis, true) // Stream completes after a long delay
        ).iterator)

      val end = recoveringIndexer.start(() => testIndexer.subscribe())
      scheduler.scheduleOnce(100.millis, () => recoveringIndexer.close())

      end map { _ =>
        List(testIndexer.actions.toArray: _*) should contain theSameElementsInOrderAs List[
          IndexerEvent](
          EventSubscribeCalled("A"),
          EventSubscribeSuccess("A"),
          EventStopCalled("A")
        )
      }
    }

    "recover failures" in {
      val recoveringIndexer = new RecoveringIndexer(actorSystem.scheduler, 10.millis, 1.second)
      // Subscribe fails, then the stream fails, then the stream completes without errors.
      val testIndexer = new TestIndexer(
        List(
          SubscribeResult("A", 10.millis, false, 10.millis, true), // Subscribe fails
          SubscribeResult("B", 10.millis, true, 10.millis, false), // Stream fails
          SubscribeResult("C", 10.millis, true, 10.millis, true) // Stream completes
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
          EventStreamComplete("C")
        )
      }
    }

    "respect restart delay" in {
      val recoveringIndexer = new RecoveringIndexer(actorSystem.scheduler, 500.millis, 1.second)
      // Subscribe fails, then the stream completes without errors. Note the restart delay of 500ms.
      val testIndexer = new TestIndexer(
        List(
          SubscribeResult("A", 0.millis, false, 0.millis, true), // Subscribe fails
          SubscribeResult("B", 0.millis, true, 0.millis, true) // Stream completes
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
          EventStreamComplete("B")
        )
      }
    }
  }
}
