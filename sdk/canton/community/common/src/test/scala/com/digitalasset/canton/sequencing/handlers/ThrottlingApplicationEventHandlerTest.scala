// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import cats.syntax.functor.*
import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.{SequencerClientHistograms, SequencerClientMetrics}
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.sequencing.{
  AsyncResult,
  HandlerResult,
  PossiblyIgnoredApplicationHandler,
  PossiblyIgnoredEnvelopeBox,
  SubscriptionStart,
}
import com.digitalasset.canton.store.SequencedEventStore.IgnoredSequencedEvent
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, FutureHelpers, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** This tests checks that the ThrottlingApplicationEventHandler behaves as expected.
  */
class ThrottlingApplicationEventHandlerTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {

  s"ThrottlingApplicationEventHandler" should {
    "respect the maximumInflightEventBatches" in {
      val handler = new TestHandler(maximumInFlightEventBatches = PositiveInt.two, loggerFactory)

      val (asyncPromise0, handlerResult0) = handler.handleNext()
      val (asyncPromise1, handlerResult1) = handler.handleNext()
      // we allow two in-flight batches, so the third batch should not be dispatched until one of the previous batches
      // have completed.
      val (asyncPromise2, handlerResult2) = handler.handleNext()

      // the first two events passed through synchronous processing but their async result is not complete
      handlerResult0.futureValueUS.unwrap.isCompleted shouldBe false
      handlerResult1.futureValueUS.unwrap.isCompleted shouldBe false
      // the third event has not yet passed through synchronous processing
      handlerResult2.unwrap.isCompleted shouldBe false

      // scheduling another event is not expected to be done by the sequencer client since it is being backpressured,
      // but we test it here regardless.
      loggerFactory.assertThrowsAndLogs[IllegalStateException](
        handler.handleNext(),
        _.throwable.value.getMessage should include(
          "Synchronization issue: cannot handle an event while at capacity and another event already waiting"
        ),
      )

      // finish the async processing of one of the events
      asyncPromise1.outcome_(())
      // the corresponding handler result should get completed
      handlerResult1.unwrap.futureValue.discard
      always() {
        handlerResult0.futureValueUS.unwrap.isCompleted shouldBe false
        // now also the third event went through the synchronous processing stage,
        // but the async result is not yet complete
        handlerResult2.futureValueUS.unwrap.isCompleted shouldBe false
      }

      // finish the async processing of another event
      asyncPromise2.outcome_(())
      handlerResult2.unwrap.futureValue.discard

      // event 0  is still being processed asynchronously, but that shouldn't prevent
      // the throttling handler from processing further events

      val (asyncPromise3, handlerResult3) = handler.handleNext()
      asyncPromise3.outcome_(())
      handlerResult3.unwrap.futureValue.discard

      // finally we release the very first async promise
      asyncPromise0.outcome_(())
      handlerResult0.unwrap.futureValue.discard
    }
  }
}

class TestHandler(maximumInFlightEventBatches: PositiveInt, loggerFactory: NamedLoggerFactory)(
    implicit ec: ExecutionContext
) extends FutureHelpers {
  val eventsToPromises = new TrieMap[SequencerCounter, FutureUnlessShutdown[Unit]]()

  val blockingHandler = new PossiblyIgnoredApplicationHandler[ClosedEnvelope] {
    override def name: String = "blocking-test-handler"

    override def subscriptionStartsAt(
        start: SubscriptionStart,
        synchronizerTimeTracker: SynchronizerTimeTracker,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = ???

    override def apply(events: PossiblyIgnoredEnvelopeBox[ClosedEnvelope]): HandlerResult =
      FutureUnlessShutdown.pure(
        AsyncResult(
          FutureUnlessShutdown
            .sequence(
              events.value.flatMap(e => eventsToPromises.get(e.counter))
            )
            .void
        )
      )

  }

  val metrics = new SequencerClientMetrics(
    new SequencerClientHistograms(MetricName.Daml)(new HistogramInventory()),
    new InMemoryMetricsFactory,
  )(MetricsContext.Empty)
  val throttlingHandler = new ThrottlingApplicationEventHandler(loggerFactory).throttle(
    maximumInFlightEventBatches,
    handler = blockingHandler,
    metrics,
  )

  private def mkEvent(offset: Long) = IgnoredSequencedEvent(
    CantonTimestamp.Epoch.plusSeconds(offset),
    SequencerCounter(offset),
    None,
    None,
  )(TraceContext.empty)

  private val counter = new AtomicLong(0)

  def handleNext(
  ): (PromiseUnlessShutdown[Unit], HandlerResult) = {
    val asyncPromise = PromiseUnlessShutdown.unsupervised[Unit]()

    val event = mkEvent(counter.incrementAndGet())
    eventsToPromises.put(event.counter, asyncPromise.futureUS).foreach { _ =>
      throw new IllegalStateException(
        s"The event with counter ${event.counter} was already processed before"
      )
    }

    val result = throttlingHandler(Traced(Seq(event))(TraceContext.empty))
    (asyncPromise, result)
  }
}
