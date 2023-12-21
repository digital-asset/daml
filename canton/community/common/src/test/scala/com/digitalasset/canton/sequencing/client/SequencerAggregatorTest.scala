// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.SequencerAggregator.SequencerAggregatorError
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, SequencerAggregator}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
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

    "support reconfiguration to 2 out of 3" in { fixture =>
      import fixture.*

      val aggregator = mkAggregator()

      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
        .futureValueUS shouldBe Right(true)

      assertDownstreamMessage(aggregator, aliceEvents(0))

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(1))
        .futureValueUS shouldBe Right(true)

      assertDownstreamMessage(aggregator, aliceEvents(1))

      aggregator.changeMessageAggregationConfig(
        config(Set(sequencerAlice, sequencerBob, sequencerCarlos), 2)
      )

      val f1 = aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(2))

      f1.isCompleted shouldBe false

      val f2 = aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(2))
      f2.futureValueUS shouldBe Right(false)
      f1.futureValueUS shouldBe Right(true)

      aggregator.eventQueue.size() shouldBe 1
      aggregator.eventQueue.take() shouldBe aggregator
        .combine(NonEmpty(Seq, aliceEvents(2), bobEvents(2)))
        .value
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
    "pass-through the combined event only if both sequencers emitted it" in { fixture =>
      import fixture.*
      val event1 = createEvent().futureValue
      val event2 = createEvent().futureValue

      val aggregator = mkAggregator(
        config(Set(sequencerAlice, sequencerBob), sequencerTrustThreshold = 2)
      )

      assertNoMessageDownstream(aggregator)

      val f1 = aggregator
        .combineAndMergeEvent(sequencerAlice, event1)

      f1.isCompleted shouldBe false
      assertNoMessageDownstream(aggregator)

      val f2 = aggregator
        .combineAndMergeEvent(sequencerBob, event2)

      f1.futureValueUS.discard
      f2.futureValueUS.discard

      f1.isCompleted shouldBe true
      f2.isCompleted shouldBe true

      assertCombinedDownstreamMessage(aggregator, event1, event2)
      f1.futureValueUS shouldBe Right(true)
      f2.futureValueUS shouldBe Right(false)
    }

    "fail if events share timestamp but timestampOfSigningKey is different" in { fixture =>
      import fixture.*
      val event1 = createEvent(timestampOfSigningKey = Some(CantonTimestamp.Epoch)).futureValue

      /*
      The goal is to do `createEvent(timestampOfSigningKey = None).futureValue`
      But if we do, we have different salt (because of the randomness in the ExampleTransactionFactory
      used by createEvent). So instead, we build the event.
       */
      val event2 = {
        val signedContent = SignedContent.tryCreate(
          content = event1.signedEvent.content,
          signatures = event1.signedEvent.signatures,
          timestampOfSigningKey = None,
          representativeProtocolVersion = event1.signedEvent.representativeProtocolVersion,
        )

        OrdinarySequencedEvent(
          signedEvent = signedContent,
          trafficState = event1.trafficState,
        )(event1.traceContext)
      }

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

    "emit events in order when all sequencers confirmed" in { fixture =>
      import fixture.*
      val events = (1 to 2).map(s =>
        createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong)).futureValue
      )
      val aggregator = mkAggregator(
        config(Set(sequencerAlice, sequencerBob), sequencerTrustThreshold = 2)
      )

      val futures = events.map { event =>
        val f = aggregator.combineAndMergeEvent(sequencerAlice, event)
        f.isCompleted shouldBe false
        f
      }

      aggregator
        .combineAndMergeEvent(sequencerBob, events(0))
        .futureValueUS shouldBe Right(false)

      futures(0).futureValueUS shouldBe Right(true)
      futures(1).isCompleted shouldBe false

      aggregator
        .combineAndMergeEvent(sequencerBob, events(1))
        .futureValueUS shouldBe Right(false)

      futures(1).futureValueUS shouldBe Right(true)
    }

    "support reconfiguration to another sequencer" in { fixture =>
      import fixture.*

      val aggregator = mkAggregator(
        config(Set(sequencerAlice, sequencerBob), sequencerTrustThreshold = 2)
      )

      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
        .discard

      aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(0))
        .discard

      assertCombinedDownstreamMessage(aggregator, aliceEvents(0), bobEvents(0))

      aggregator.changeMessageAggregationConfig(
        config(Set(sequencerCarlos), 1)
      )

      aggregator
        .combineAndMergeEvent(sequencerCarlos, carlosEvents(1))
        .futureValueUS shouldBe Right(true)

      assertDownstreamMessage(aggregator, carlosEvents(1))
    }
  }

  "Sequencer aggregator with two out of 3 expected sequencers" should {
    "pass-through the combined event only if both sequencers emitted it" in { fixture =>
      import fixture.*

      val aggregator = mkAggregator(
        config(Set(sequencerAlice, sequencerBob, sequencerCarlos), sequencerTrustThreshold = 2)
      )

      assertNoMessageDownstream(aggregator)

      val f1 = aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))

      f1.isCompleted shouldBe false
      assertNoMessageDownstream(aggregator)

      val f2 = aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(0))

      f2.futureValueUS.discard

      f1.isCompleted shouldBe true
      f2.isCompleted shouldBe true

      assertCombinedDownstreamMessage(
        aggregator,
        aliceEvents(0),
        bobEvents(0),
      )
      f1.futureValueUS shouldBe Right(true)
      f2.futureValueUS shouldBe Right(false)

      val f3 = aggregator
        .combineAndMergeEvent(sequencerCarlos, carlosEvents(0)) // late event
      f3.isCompleted shouldBe true // should be immediately resolved
      f3.futureValueUS shouldBe Right(false)
    }

    "recover after skipping an event" in { fixture =>
      import fixture.*

      val aggregator = mkAggregator(
        config(Set(sequencerAlice, sequencerBob, sequencerCarlos), sequencerTrustThreshold = 2)
      )

      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
        .discard
      aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(0))
        .discard

      assertCombinedDownstreamMessage(aggregator, aliceEvents(0), bobEvents(0))

      aggregator
        .combineAndMergeEvent(sequencerCarlos, carlosEvents(0))
        .discard // late event

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(1))
        .discard
      aggregator
        .combineAndMergeEvent(sequencerCarlos, carlosEvents(1))
        .discard

      assertCombinedDownstreamMessage(
        aggregator,
        aliceEvents(1),
        carlosEvents(1),
      )
    }

    "support reconfiguration to 1 out of 3" in { fixture =>
      import fixture.*

      val aggregator = mkAggregator(
        config(Set(sequencerAlice, sequencerBob, sequencerCarlos), sequencerTrustThreshold = 2)
      )

      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
        .discard
      aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(0))
        .discard

      assertCombinedDownstreamMessage(
        aggregator,
        aliceEvents(0),
        bobEvents(0),
      )

      aggregator
        .combineAndMergeEvent(sequencerCarlos, carlosEvents(0))
        .discard // late event

      aggregator.changeMessageAggregationConfig(
        config(
          Set(sequencerAlice, sequencerBob, sequencerCarlos),
          1,
        )
      )
      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(1))
        .discard

      assertDownstreamMessage(aggregator, aliceEvents(1))
    }

    "support reconfiguration to 1 out of 3 while incomplete consensus" in { fixture =>
      import fixture.*

      val aggregator = mkAggregator(
        config(Set(sequencerAlice, sequencerBob, sequencerCarlos), sequencerTrustThreshold = 2)
      )
      assertNoMessageDownstream(aggregator)

      val f = aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
      f.isCompleted shouldBe false

      assertNoMessageDownstream(aggregator)

      aggregator.changeMessageAggregationConfig(
        config(Set(sequencerAlice, sequencerBob, sequencerCarlos), sequencerTrustThreshold = 1)
      )

      // consensus requirement is changed which is enough to push the message out

      assertDownstreamMessage(aggregator, aliceEvents(0))
      f.futureValueUS shouldBe Right(true)
    }
  }

  "Sequencer aggregator with 3 out of 3 expected sequencers" should {
    "support reconfiguration to 1 out of 3 while overfulfilled consensus" in { fixture =>
      import fixture.*

      val aggregator = mkAggregator(
        config(Set(sequencerAlice, sequencerBob, sequencerCarlos), sequencerTrustThreshold = 3)
      )
      assertNoMessageDownstream(aggregator)

      val f1 = aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
      f1.isCompleted shouldBe false

      val f2 = aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(0))
      f2.isCompleted shouldBe false

      assertNoMessageDownstream(aggregator)

      aggregator.changeMessageAggregationConfig(
        config(Set(sequencerAlice, sequencerBob, sequencerCarlos), sequencerTrustThreshold = 1)
      )

      // consensus requirement is changed which more than enough (2 are there, 1 is required) to push the message out
      // we do accumulate all signatures still as sequencers are still expected ones
      assertCombinedDownstreamMessage(aggregator, aliceEvents(0), bobEvents(0))
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

  private def assertCombinedDownstreamMessage(
      aggregator: SequencerAggregator,
      events: OrdinarySerializedEvent*
  ): Assertion = clue("Expected a single combined downstream message from multiple sequencers") {
    aggregator.eventQueue.size() shouldBe 1
    aggregator.eventQueue.take() shouldBe combinedMessage(aggregator, events *)
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
