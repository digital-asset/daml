// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.SequencerAggregator.SequencerAggregatorError
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, SequencerAggregator}
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  ProtocolVersionChecksFixtureAnyWordSpec,
}
import com.google.protobuf.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.FixtureAnyWordSpec
import org.scalatest.{Assertion, Outcome}

import scala.concurrent.{Future, Promise}

class SequencerAggregatorTest
    extends FixtureAnyWordSpec
    with BaseTest
    with ScalaFutures
    with ProtocolVersionChecksFixtureAnyWordSpec
    with HasExecutionContext {

  override type FixtureParam = SequencedEventTestFixture

  override def withFixture(test: OneArgTest): Outcome =
    ResourceUtil.withResource(
      new SequencedEventTestFixture(
        loggerFactory,
        testedProtocolVersion,
        timeouts,
        futureSupervisor,
      )
    ) { env => withFixture(test.toNoArgTest(env)) }

  "Single sequencer aggregator" should {
    "pass-through the event" in { fixture =>
      import fixture.*
      val event = createEvent().futureValue

      val aggregator = mkAggregator()

      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerAlice, event)
        .futureValueUS shouldBe Right(true)

      aggregator.eventQueue.take() shouldBe event
    }

    "pass-through events in sequence" in { fixture =>
      import fixture.*
      val events = (1 to 100).map(s =>
        createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong)).futureValue
      )

      val aggregator = mkAggregator()

      events.foreach { event =>
        aggregator
          .combineAndMergeEvent(sequencerAlice, event)
          .futureValueUS shouldBe Right(true)
        aggregator.eventQueue.take() shouldBe event
      }
    }

    "block on queue is full" in { fixture =>
      import fixture.*
      val events = (1 to 2).map(s =>
        createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong)).futureValue
      )

      val aggregator = mkAggregator()

      assertNoMessageDownstream(aggregator)

      events.foreach { event =>
        aggregator
          .combineAndMergeEvent(sequencerAlice, event)
          .futureValueUS shouldBe Right(true)
      }

      val blockingEvent = createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(3L)).futureValue

      val p = Promise[Future[Either[SequencerAggregatorError, Boolean]]]()
      p.completeWith(
        Future(
          aggregator
            .combineAndMergeEvent(sequencerAlice, blockingEvent)
            .failOnShutdown
        )
      )
      always() {
        p.isCompleted shouldBe false
      }
      aggregator.eventQueue.take() shouldBe events(0)
      eventually() {
        p.isCompleted shouldBe true
      }
    }

    "support reconfiguration to another sequencer" in { fixture =>
      import fixture.*

      val aggregator = mkAggregator(
        config(Set(sequencerAlice), 1)
      )

      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
        .futureValueUS shouldBe Right(true)

      assertDownstreamMessage(aggregator, aliceEvents(0))

      aggregator.changeMessageAggregationConfig(
        config(Set(sequencerBob), 1)
      )

      aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(0)) // arrived late event which we ignore
        .futureValueUS shouldBe Right(false)
      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(1))
        .futureValueUS shouldBe Right(true)
      assertDownstreamMessage(aggregator, bobEvents(1))
    }
  }

  "Sequencer aggregator with two expected sequencers" should {
    "fail if events share timestamp but timestampOfSigningKey is different" in { fixture =>
      import fixture.*
      val event1 = createEvent(timestampOfSigningKey = Some(CantonTimestamp.Epoch)).futureValue
      val event2 = createEvent(timestampOfSigningKey = None).futureValue

      val aggregator = mkAggregator(
        config(Set(sequencerAlice, sequencerBob), sequencerTrustThreshold = 2)
      )
      val f1 = aggregator
        .combineAndMergeEvent(sequencerAlice, event1)

      f1.isCompleted shouldBe false
      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerBob, event2)
        .futureValueUS shouldBe Left(
        SequencerAggregatorError.NotTheSameTimestampOfSigningKey(
          NonEmpty(Set, Some(CantonTimestamp.Epoch), None)
        )
      )
    }

    "fail if events share timestamp but content is different" in { fixture =>
      import fixture.*
      val event1 = createEvent().futureValue
      val event2 = createEvent(serializedOverride = Some(ByteString.EMPTY)).futureValue

      val aggregator = mkAggregator(
        config(Set(sequencerAlice, sequencerBob), sequencerTrustThreshold = 2)
      )
      val f1 = aggregator
        .combineAndMergeEvent(sequencerAlice, event1)

      f1.isCompleted shouldBe false
      assertNoMessageDownstream(aggregator)

      val hashes = NonEmpty(
        Set,
        hash(event1.signedEvent.content.toByteString),
        hash(event2.signedEvent.content.toByteString),
      )

      aggregator
        .combineAndMergeEvent(sequencerBob, event2)
        .futureValueUS shouldBe Left(SequencerAggregatorError.NotTheSameContentHash(hashes))
    }
  }

  private def assertDownstreamMessage(
      aggregator: SequencerAggregator,
      message: OrdinarySerializedEvent,
  ): Assertion = {
    clue("Expected a single downstream message") {
      aggregator.eventQueue.size() shouldBe 1
      aggregator.eventQueue.take() shouldBe message
    }
  }

  private def assertNoMessageDownstream(aggregator: SequencerAggregator): Assertion =
    clue("Expected no downstream messages") {
      aggregator.eventQueue.size() shouldBe 0
    }

  private def combinedMessage(
      aggregator: SequencerAggregator,
      events: OrdinarySerializedEvent*
  ): OrdinarySerializedEvent = {
    aggregator
      .combine(NonEmptyUtil.fromUnsafe(events.toList))
      .value
  }
}
