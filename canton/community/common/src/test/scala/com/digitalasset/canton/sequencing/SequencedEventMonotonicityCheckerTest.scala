// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import akka.stream.scaladsl.{Keep, Sink, Source}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.SequencedEventMonotonicityChecker.MonotonicityFailureException
import com.digitalasset.canton.sequencing.client.SequencedEventTestFixture
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.AkkaUtil.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, ResourceUtil}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  ProtocolVersionChecksFixtureAnyWordSpec,
  SequencerCounter,
}
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
    ) { env => withFixture(test.toNoArgTest(env)) }

  private def mkHandler(): CapturingApplicationHandler = new CapturingApplicationHandler

  "handler" should {
    "pass through monotonically increasing events" in { env =>
      import env.*

      val checker = new SequencedEventMonotonicityChecker(
        bobEvents(0).counter,
        bobEvents(0).timestamp,
        loggerFactory,
      )
      val handler = mkHandler()
      val checkedHandler = checker.handler(handler)
      val (batch1, batch2) = bobEvents.splitAt(2)

      checkedHandler(Traced(batch1)).futureValueUS.unwrap.futureValueUS
      checkedHandler(Traced(batch2)).futureValueUS.unwrap.futureValueUS
      handler.invocations.get.flatMap(_.value) shouldBe bobEvents
    }

    "detect gaps in sequencer counters" in { env =>
      import env.*

      val checker = new SequencedEventMonotonicityChecker(
        bobEvents(0).counter,
        bobEvents(0).timestamp,
        loggerFactory,
      )
      val handler = mkHandler()
      val checkedHandler = checker.handler(handler)
      val (batch1, batch2) = bobEvents.splitAt(2)

      checkedHandler(Traced(batch1)).futureValueUS.unwrap.futureValueUS
      loggerFactory.assertThrowsAndLogs[MonotonicityFailureException](
        checkedHandler(Traced(batch2.drop(1))).futureValueUS.unwrap.futureValueUS,
        _.errorMessage should include(ErrorUtil.internalErrorMessage),
      )
    }

    "detect non-monotonic timestamps" in { env =>
      import env.*

      val event1 = createEvent(
        timestamp = CantonTimestamp.ofEpochSecond(2),
        counter = 2L,
      ).futureValue
      val event2 = createEvent(
        timestamp = CantonTimestamp.ofEpochSecond(2),
        counter = 3L,
      ).futureValue

      val checker = new SequencedEventMonotonicityChecker(
        SequencerCounter(2L),
        CantonTimestamp.MinValue,
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
        bobEvents(0).counter,
        bobEvents(0).timestamp,
        loggerFactory,
      )
      val eventsF = Source(bobEvents)
        .withUniqueKillSwitchMat()(Keep.left)
        .via(checker.flow)
        .toMat(Sink.seq)(Keep.right)
        .run()
      eventsF.futureValue.map(_.unwrap) shouldBe bobEvents
    }

    "kill the stream upon a gap in the counters" in { env =>
      import env.*

      val checker = new SequencedEventMonotonicityChecker(
        bobEvents(0).counter,
        bobEvents(0).timestamp,
        loggerFactory,
      )
      val (batch1, batch2) = bobEvents.splitAt(2)
      val eventsF = loggerFactory.assertLogs(
        Source(batch1 ++ batch2.drop(1))
          .withUniqueKillSwitchMat()(Keep.left)
          .via(checker.flow)
          .toMat(Sink.seq)(Keep.right)
          .run(),
        _.errorMessage should include(
          "Sequencer counters and timestamps do not increase monotonically"
        ),
      )
      eventsF.futureValue.map(_.unwrap) shouldBe batch1
    }

    "detect non-monotonic timestamps" in { env =>
      import env.*

      val event1 = createEvent(
        timestamp = CantonTimestamp.ofEpochSecond(2),
        counter = 2L,
      ).futureValue
      val event2 = createEvent(
        timestamp = CantonTimestamp.ofEpochSecond(2),
        counter = 3L,
      ).futureValue

      val checker = new SequencedEventMonotonicityChecker(
        SequencerCounter(2L),
        CantonTimestamp.MinValue,
        loggerFactory,
      )
      val eventsF = loggerFactory.assertLogs(
        Source(Seq(event1, event2))
          .withUniqueKillSwitchMat()(Keep.left)
          .via(checker.flow)
          .toMat(Sink.seq)(Keep.right)
          .run(),
        _.errorMessage should include(
          "Sequencer counters and timestamps do not increase monotonically"
        ),
      )
      eventsF.futureValue.map(_.unwrap) shouldBe Seq(event1)
    }
  }
}

object SequencedEventMonotonicityCheckerTest {
  class CapturingApplicationHandler()
      extends ApplicationHandler[OrdinaryEnvelopeBox, ClosedEnvelope] {
    val invocations =
      new AtomicReference[Seq[BoxedEnvelope[OrdinaryEnvelopeBox, ClosedEnvelope]]](Seq.empty)

    override def name: String = "capturing-application-handler"
    override def subscriptionStartsAt(
        start: SubscriptionStart,
        domainTimeTracker: DomainTimeTracker,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

    override def apply(boxed: BoxedEnvelope[OrdinaryEnvelopeBox, ClosedEnvelope]): HandlerResult = {
      invocations
        .getAndUpdate(_ :+ boxed)
        .discard[Seq[BoxedEnvelope[OrdinaryEnvelopeBox, ClosedEnvelope]]]
      HandlerResult.done
    }
  }
}
