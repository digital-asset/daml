// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.SequencedEventMonotonicityChecker.MonotonicityFailureException
import com.digitalasset.canton.sequencing.client.SequencedEventTestFixture
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, ResourceUtil}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  ProtocolVersionChecksFixtureAnyWordSpec,
}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

import java.util.concurrent.atomic.AtomicReference

class SequencedEventMonotonicityCheckerTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksFixtureAnyWordSpec {
  import SequencedEventMonotonicityCheckerTest.*

  override protected type FixtureParam = SequencedEventTestFixture

  override protected def withFixture(test: OneArgTest): Outcome =
    ResourceUtil.withResource(
      new SequencedEventTestFixture(
        loggerFactory,
        testedProtocolVersion,
        timeouts,
        futureSupervisor,
      )
    )(env => withFixture(test.toNoArgTest(env)))

  private def mkHandler(): CapturingApplicationHandler = new CapturingApplicationHandler

  "handler" should {
    "pass through monotonically increasing events" in { env =>
      import env.*

      val checker = new SequencedEventMonotonicityChecker(
        previousEventTimestamp = None,
        loggerFactory,
      )
      val handler = mkHandler()
      val checkedHandler = checker.handler(handler)
      val (batch1, batch2) = bobEvents.splitAt(2)

      checkedHandler(Traced(batch1)).futureValueUS.unwrap.futureValueUS
      checkedHandler(Traced(batch2)).futureValueUS.unwrap.futureValueUS
      handler.invocations.get.flatMap(_.value) shouldBe bobEvents
    }

    "detect non-monotonic timestamps" in { env =>
      import env.*

      val event1 = createEvent(
        timestamp = CantonTimestamp.ofEpochSecond(2),
        previousTimestamp = Some(CantonTimestamp.ofEpochSecond(1)),
        counter = 2L,
      ).futureValueUS
      val event2 = createEvent(
        timestamp = CantonTimestamp.ofEpochSecond(2),
        previousTimestamp = Some(CantonTimestamp.ofEpochSecond(2)),
        counter = 3L,
      ).futureValueUS

      val checker = new SequencedEventMonotonicityChecker(
        previousEventTimestamp = Some(CantonTimestamp.ofEpochSecond(1)),
        loggerFactory,
      )
      val handler = mkHandler()
      val checkedHandler = checker.handler(handler)

      checkedHandler(Traced(Seq(event1))).futureValueUS.unwrap.futureValueUS
      loggerFactory.assertThrowsAndLogs[MonotonicityFailureException](
        checkedHandler(Traced(Seq(event2))).futureValueUS.unwrap.futureValueUS,
        _.errorMessage should include(ErrorUtil.internalErrorMessage),
      )
    }
  }

  "flow" should {
    "pass through monotonically increasing events" in { env =>
      import env.*

      val checker = new SequencedEventMonotonicityChecker(
        previousEventTimestamp = None,
        loggerFactory,
      )
      val eventsF = Source(bobEvents)
        .map(Right(_))
        .withUniqueKillSwitchMat()(Keep.left)
        .via(checker.flow)
        .toMat(Sink.seq)(Keep.right)
        .run()
      eventsF.futureValue.map(_.value) shouldBe bobEvents.map(Right(_))
    }

    "detect non-monotonic timestamps" in { env =>
      import env.*

      val event1 = createEvent(
        timestamp = CantonTimestamp.ofEpochSecond(2),
        previousTimestamp = Some(CantonTimestamp.ofEpochSecond(1)),
        counter = 2L,
      ).futureValueUS
      val event2 = createEvent(
        timestamp = CantonTimestamp.ofEpochSecond(2),
        previousTimestamp = Some(CantonTimestamp.ofEpochSecond(2)),
        counter = 3L,
      ).futureValueUS

      val checker = new SequencedEventMonotonicityChecker(
        previousEventTimestamp = Some(CantonTimestamp.ofEpochSecond(1)),
        loggerFactory,
      )
      val eventsF = loggerFactory.assertLogs(
        Source(Seq(event1, event2))
          .map(Right(_))
          .withUniqueKillSwitchMat()(Keep.left)
          .via(checker.flow)
          .toMat(Sink.seq)(Keep.right)
          .run(),
        _.errorMessage should include(
          "Timestamps do not increase monotonically or previous event timestamp does not match."
        ),
      )
      eventsF.futureValue.map(_.value) shouldBe Seq(Right(event1))
    }
  }
}

object SequencedEventMonotonicityCheckerTest {
  class CapturingApplicationHandler()
      extends ApplicationHandler[SequencedEnvelopeBox, ClosedEnvelope] {
    val invocations =
      new AtomicReference[Seq[BoxedEnvelope[SequencedEnvelopeBox, ClosedEnvelope]]](Seq.empty)

    override def name: String = "capturing-application-handler"
    override def subscriptionStartsAt(
        start: SubscriptionStart,
        synchronizerTimeTracker: SynchronizerTimeTracker,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

    override def apply(
        boxed: BoxedEnvelope[SequencedEnvelopeBox, ClosedEnvelope]
    ): HandlerResult = {
      invocations
        .getAndUpdate(_ :+ boxed)
        .discard[Seq[BoxedEnvelope[SequencedEnvelopeBox, ClosedEnvelope]]]
      HandlerResult.done
    }
  }
}
