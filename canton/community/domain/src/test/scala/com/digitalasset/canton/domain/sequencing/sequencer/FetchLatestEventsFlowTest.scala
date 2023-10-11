// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.stream.scaladsl.{Keep, Sink, SinkQueueWithCancel, Source}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.util.{AkkaUtil, MonadUtil}
import com.digitalasset.canton.{BaseTest, DiscardOps, HasExecutionContext}
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}

@nowarn("msg=match may not be exhaustive")
class FetchLatestEventsFlowTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext {
  case class State(timestamp: CantonTimestamp)
  case class Event(timestamp: CantonTimestamp)

  case class LookupCall(
      calledP: Promise[State] = Promise(),
      resultP: Promise[(State, Seq[Event])] = Promise(),
      name: String = "LookupCall",
  ) {
    lazy val calledF = calledP.future

    def returnsSingleEvent(ts: CantonTimestamp): Unit = resultP.success((State(ts), Seq(Event(ts))))
    def returnsNoEvents(): Unit =
      calledP.future.foreach(state => resultP.success((state, Seq.empty)))
  }

  class Env extends FlagCloseable {
    override val timeouts = FetchLatestEventsFlowTest.this.timeouts
    implicit val system = AkkaUtil.createActorSystem(loggerFactory.threadName)
    val logger = FetchLatestEventsFlowTest.this.logger
    val lookupEventsQueue = new ConcurrentLinkedQueue[LookupCall]()
    val lookupCount = new AtomicInteger()
    def ts(epochSecondOffset: Int): CantonTimestamp =
      CantonTimestamp.Epoch.plusSeconds(epochSecondOffset.toLong)

    def mockLookup(name: String = "LookupCall"): LookupCall = {
      val call = LookupCall(name = name)
      if (!lookupEventsQueue.offer(call)) fail("failed to enqueue mocked event lookup")
      call
    }

    def mockLookups(count: Int): Seq[LookupCall] =
      (0.until(count)).map(n => mockLookup(name = s"lookup-$n"))

    def lookupEvents(state: State): Future[(State, Seq[Event])] = {
      lookupCount.incrementAndGet()
      val call = Option(lookupEventsQueue.poll()).getOrElse(fail(s"unexpected lookup for $state"))
      call.calledP.success(state)
      call.resultP.future
    }

    def waitForAll(calls: LookupCall*): Future[Unit] =
      MonadUtil.sequentialTraverse_(calls)(_.calledF)

    val initialState = State(CantonTimestamp.Epoch)
    def create[Mat1, Mat2](
        source: Source[ReadSignal, Mat1],
        sink: Sink[Event, Mat2],
    ): (Mat1, Mat2) =
      AkkaUtil.runSupervised(
        logger.error("LatestEventsFlowTest failed", _), {
          source
            .via(
              FetchLatestEventsFlow[Event, State](
                initialState,
                lookupEvents,
                (_, events) => events.isEmpty,
              )
            )
            .toMat(sink)(Keep.both)
        },
      )

    def create[Mat1](source: Source[ReadSignal, Mat1]): (Mat1, SinkQueueWithCancel[Event]) =
      create(source, Sink.queue())

    def pullEvent(queue: SinkQueueWithCancel[Event]): Future[Event] =
      queue.pull().map(_.getOrElse(fail("Expected event")))

    def pullCompleted(queue: SinkQueueWithCancel[Event]): Future[Unit] =
      queue.pull() map {
        case Some(_) => fail("Expected queue to be completed")
        case None => ()
      }

    override protected def onClosed(): Unit =
      Lifecycle.close(
        Lifecycle.toCloseableActorSystem(system, logger, timeouts)
      )(logger)
  }

  override type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()
    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly (env.close())
  }

  "immediately fetches to latest regardless of being updated" in { env =>
    import env.*
    val calls @ Seq(call1, call2, call3) = mockLookups(3)
    // find one event on the first call
    call1.returnsSingleEvent(ts(5))
    call2.returnsNoEvents()

    val (_, eventQueue) = create(Source.empty[ReadSignal])

    for {
      _ <- call1.calledF
      _ <- call2.calledF
      event <- pullEvent(eventQueue)
      _ = event shouldBe (Event(ts(5)))
      finalCallState <- call2.calledF
    } yield finalCallState shouldBe State(ts(5))
  }

  "keeps fetching until we've reached the latest timestamp" in { env =>
    import env.*

    val lookups @ Seq(lookup1, lookup2, lookup3) = mockLookups(3)
    lookup1.returnsSingleEvent(ts(5))
    lookup2.returnsSingleEvent(ts(8))
    lookup3.returnsNoEvents()

    create(Source.empty[ReadSignal]).discard

    for {
      _ <- waitForAll(lookups: _*)
      lastCall <- lookup3.calledF
    } yield lastCall shouldBe State(ts(8))
  }

  "drops read signals if read events take a long period" in { env =>
    import env.*

    val lookups = mockLookups(100)
    val lookup1 = lookups.headOption.value

    // complete remaining
    lookups.drop(1).foreach(_.returnsNoEvents())

    val (_, eventF) = create(Source(0 until 100).map(_ => ReadSignal), Sink.seq)

    for {
      _ <- lookup1.calledF
      _ <- {
        // wait for a while
        val p = Promise[Unit]()
        system.scheduler.scheduleOnce(500.millis)(p.success(()))
        p.future
      }
      _ = lookup1.returnsSingleEvent(ts(9))
      _ <- {
        // wait for a while
        val p = Promise[Unit]()
        system.scheduler.scheduleOnce(500.millis)(p.success(()))
        p.future
      }
      _ <- eventF
    } yield lookupCount.get() shouldBe <(10)
  }
}
