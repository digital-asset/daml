// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{KillSwitches, QueueOfferResult}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{Fingerprint, Signature}
import com.digitalasset.canton.sequencing.SequencerAggregatorAkka.HasSequencerSubscriptionFactoryAkka
import com.digitalasset.canton.sequencing.SequencerAggregatorAkkaTest.Config
import com.digitalasset.canton.sequencing.client.TestSequencerSubscriptionFactoryAkka.{
  Error,
  Event,
  Failure,
}
import com.digitalasset.canton.sequencing.client.TestSubscriptionError.UnretryableError
import com.digitalasset.canton.sequencing.client.{
  ResilientSequencerSubscription,
  SequencedEventTestFixture,
  SequencedEventValidator,
  SequencedEventValidatorImpl,
  SequencerSubscriptionFactoryAkka,
  TestSequencerSubscriptionFactoryAkka,
  TestSubscriptionError,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, SequencerId}
import com.digitalasset.canton.util.OrderedBucketMergeHub.{ActiveSourceTerminated, NewConfiguration}
import com.digitalasset.canton.util.{OrderedBucketMergeConfig, ResourceUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  ProtocolVersionChecksFixtureAnyWordSpec,
  SequencerCounter,
}
import com.google.protobuf.ByteString
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

class SequencerAggregatorAkkaTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksFixtureAnyWordSpec {

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

  private def mkAggregatorAkka(
      validator: SequencedEventValidator =
        SequencedEventValidator.noValidation(DefaultTestIdentities.domainId, warn = false)
  )(implicit fixture: FixtureParam): SequencerAggregatorAkka =
    new SequencerAggregatorAkka(
      validator,
      PositiveInt.one,
      fixture.subscriberCryptoApi.pureCrypto,
      loggerFactory,
      enableInvariantCheck = true,
    )

  private def fakeSignatureFor(name: String): Signature =
    SymbolicCrypto.signature(
      ByteString.EMPTY,
      Fingerprint.tryCreate(name),
    )

  // Sort the signatures by the fingerprint of the key to get a deterministic ordering
  private def normalize(event: OrdinarySerializedEvent): OrdinarySerializedEvent =
    event.copy(signedEvent =
      event.signedEvent.copy(signatures =
        event.signedEvent.signatures.sortBy(_.signedBy.toProtoPrimitive)
      )
    )(event.traceContext)

  private def mkEvents(start: SequencerCounter, amount: Int): Seq[Event] =
    (0 until amount).map(i => Event(start + i))

  "aggregator" should {
    "pass through events from a single sequencer subscription" in { implicit fixture =>
      import fixture.*

      val aggregator = mkAggregatorAkka()
      val factory = new TestSequencerSubscriptionFactoryAkka(loggerFactory)
      factory.add(mkEvents(SequencerCounter.Genesis, 3) *)

      val config = OrderedBucketMergeConfig(
        PositiveInt.one,
        NonEmpty(Map, sequencerAlice -> Config("")(factory)),
      )
      val configSource =
        Source.single(config).concat(Source.never).viaMat(KillSwitches.single)(Keep.right)

      val ((killSwitch, doneF), sink) = configSource
        .viaMat(aggregator.aggregateFlow(Left(SequencerCounter.Genesis)))(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(5)
      sink.expectNext() shouldBe Left(NewConfiguration(config, SequencerCounter.Genesis - 1L))
      sink.expectNext().value shouldBe
        Event(SequencerCounter.Genesis).asOrdinarySerializedEvent
      sink.expectNext().value shouldBe
        Event(SequencerCounter.Genesis + 1L).asOrdinarySerializedEvent
      sink.expectNext().value shouldBe
        Event(SequencerCounter.Genesis + 2L).asOrdinarySerializedEvent
      sink.expectNoMessage()
      killSwitch.shutdown()
      sink.expectComplete()
      doneF.futureValue
    }

    "log the error when a subscription signals an error" in { implicit fixture =>
      import fixture.*
      val aggregator = mkAggregatorAkka()
      val factory = new TestSequencerSubscriptionFactoryAkka(loggerFactory)
      factory.add(Error(UnretryableError))
      val config = OrderedBucketMergeConfig(
        PositiveInt.one,
        NonEmpty(Map, sequencerAlice -> Config("")(factory)),
      )
      val configSource =
        Source.single(config).concat(Source.never).viaMat(KillSwitches.single)(Keep.right)

      val ((killSwitch, doneF), sink) = loggerFactory.assertLogs(
        {
          val (handle, sink) = configSource
            .viaMat(aggregator.aggregateFlow(Left(SequencerCounter.Genesis)))(Keep.both)
            .toMat(TestSink.probe)(Keep.both)
            .run()

          sink.request(5)
          sink.expectNext() shouldBe Left(NewConfiguration(config, SequencerCounter.Genesis - 1L))
          sink.expectNext() shouldBe Left(ActiveSourceTerminated(sequencerAlice, None))

          (handle, sink)
        },
        _.warningMessage should include(
          s"Sequencer subscription for $sequencerAlice failed with $UnretryableError"
        ),
      )
      killSwitch.shutdown()
      sink.expectComplete()
      doneF.futureValue
    }

    "propagate the exception from a subscription" in { implicit fixture =>
      import fixture.*
      val aggregator = mkAggregatorAkka()
      val factory = new TestSequencerSubscriptionFactoryAkka(loggerFactory)
      val ex = new Exception("Alice subscription failure")
      factory.add(Failure(ex))
      val config = OrderedBucketMergeConfig(
        PositiveInt.one,
        NonEmpty(Map, sequencerAlice -> Config("")(factory)),
      )
      val configSource =
        Source.single(config).concat(Source.never).viaMat(KillSwitches.single)(Keep.right)

      val ((killSwitch, doneF), sink) = loggerFactory.assertLogs(
        {
          val (handle, sink) = configSource
            .viaMat(aggregator.aggregateFlow(Left(SequencerCounter.Genesis)))(Keep.both)
            .toMat(TestSink.probe)(Keep.both)
            .run()

          sink.request(5)
          sink.expectNext() shouldBe Left(NewConfiguration(config, SequencerCounter.Genesis - 1L))
          sink.expectNext() shouldBe Left(ActiveSourceTerminated(sequencerAlice, Some(ex)))

          (handle, sink)
        },
        _.errorMessage should include(s"Sequencer subscription for $sequencerAlice failed"),
      )
      killSwitch.shutdown()
      sink.expectComplete()
      doneF.futureValue
    }

    "support reconfiguration for single sequencers" in { implicit fixture =>
      import fixture.*

      val aggregator = mkAggregatorAkka()
      val ((source, doneF), sink) = Source
        .queue[OrderedBucketMergeConfig[SequencerId, Config]](1)
        .viaMat(aggregator.aggregateFlow(Left(SequencerCounter.Genesis)))(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val factory = new TestSequencerSubscriptionFactoryAkka(loggerFactory)
      factory.add(mkEvents(SequencerCounter.Genesis, 3) *)
      factory.add(mkEvents(SequencerCounter.Genesis + 2, 3) *)
      val config1 = OrderedBucketMergeConfig(
        PositiveInt.one,
        NonEmpty(Map, sequencerAlice -> Config("V1")(factory)),
      )
      val config2 = OrderedBucketMergeConfig(
        PositiveInt.one,
        NonEmpty(Map, sequencerAlice -> Config("V2")(factory)),
      )
      source.offer(config1) shouldBe QueueOfferResult.Enqueued
      sink.request(10)
      sink.expectNext() shouldBe Left(NewConfiguration(config1, SequencerCounter.Genesis - 1))
      sink.expectNext().value shouldBe
        Event(SequencerCounter.Genesis).asOrdinarySerializedEvent
      sink.expectNext().value shouldBe
        Event(SequencerCounter.Genesis + 1).asOrdinarySerializedEvent
      sink.expectNext().value shouldBe
        Event(SequencerCounter.Genesis + 2).asOrdinarySerializedEvent
      sink.expectNoMessage()
      source.offer(config2) shouldBe QueueOfferResult.Enqueued
      sink.expectNext() shouldBe Left(NewConfiguration(config2, SequencerCounter.Genesis + 2))
      sink.expectNext().value shouldBe
        Event(SequencerCounter.Genesis + 3).asOrdinarySerializedEvent
      sink.expectNext().value shouldBe
        Event(SequencerCounter.Genesis + 4).asOrdinarySerializedEvent
      sink.expectNoMessage()
      source.complete()
      sink.expectComplete()
      doneF.futureValue
    }

    "pass through the event only if sufficiently many sequencer IDs send it" onlyRunWithOrGreaterThan
      ProtocolVersion.CNTestNet in { implicit fixture =>
        import fixture.*

        val aggregator = mkAggregatorAkka()

        val factoryAlice = new TestSequencerSubscriptionFactoryAkka(loggerFactory)
        val factoryBob = new TestSequencerSubscriptionFactoryAkka(loggerFactory)
        val factoryCarlos = new TestSequencerSubscriptionFactoryAkka(loggerFactory)

        val signatureAlice = fakeSignatureFor("Alice")
        val signatureBob = fakeSignatureFor("Bob")
        val signatureCarlos = fakeSignatureFor("Carlos")

        val events = mkEvents(SequencerCounter.Genesis, 3)
        factoryAlice.add(events.take(1).map(_.copy(signatures = NonEmpty(Set, signatureAlice))) *)
        factoryBob.add(events.slice(1, 2).map(_.copy(signatures = NonEmpty(Set, signatureBob))) *)
        factoryCarlos.add(events.take(3).map(_.copy(signatures = NonEmpty(Set, signatureCarlos))) *)

        val config = OrderedBucketMergeConfig(
          PositiveInt.tryCreate(2),
          NonEmpty(
            Map,
            sequencerAlice -> Config("Alice")(factoryAlice),
            sequencerBob -> Config("Bob")(factoryBob),
            sequencerCarlos -> Config("Carlos")(factoryCarlos),
          ),
        )
        val configSource =
          Source.single(config).concat(Source.never).viaMat(KillSwitches.single)(Keep.right)

        val ((killSwitch, doneF), sink) = configSource
          .viaMat(aggregator.aggregateFlow(Left(SequencerCounter.Genesis)))(Keep.both)
          .toMat(TestSink.probe)(Keep.both)
          .run()

        sink.request(4)
        sink.expectNext() shouldBe Left(NewConfiguration(config, SequencerCounter.Genesis - 1L))
        normalize(sink.expectNext().value) shouldBe normalize(
          Event(
            SequencerCounter.Genesis,
            NonEmpty(Set, signatureAlice, signatureCarlos),
          ).asOrdinarySerializedEvent
        )
        normalize(sink.expectNext().value) shouldBe normalize(
          Event(
            SequencerCounter.Genesis + 1L,
            NonEmpty(Set, signatureBob, signatureCarlos),
          ).asOrdinarySerializedEvent
        )
        sink.expectNoMessage()
        killSwitch.shutdown()
        sink.expectComplete()
        doneF.futureValue
      }

    "support reconfiguring the threshold and sequencers" onlyRunWithOrGreaterThan
      ProtocolVersion.CNTestNet in { implicit fixture =>
        import fixture.*

        val validator = new SequencedEventValidatorImpl(
          // Disable signature checking
          unauthenticated = true,
          optimistic = false,
          defaultDomainId,
          testedProtocolVersion,
          subscriberCryptoApi,
          loggerFactory,
          timeouts,
        )
        val initialCounter = SequencerCounter(10)
        val aggregator = mkAggregatorAkka(validator)
        val ((source, doneF), sink) = Source
          .queue[OrderedBucketMergeConfig[SequencerId, Config]](1)
          .viaMat(
            aggregator.aggregateFlow(Right(Event(initialCounter).asOrdinarySerializedEvent))
          )(Keep.both)
          .toMat(TestSink.probe)(Keep.both)
          .run()

        val factoryAlice = new TestSequencerSubscriptionFactoryAkka(loggerFactory)
        val factoryBob = new TestSequencerSubscriptionFactoryAkka(loggerFactory)
        val factoryCarlos = new TestSequencerSubscriptionFactoryAkka(loggerFactory)

        val config1 = OrderedBucketMergeConfig(
          PositiveInt.tryCreate(2),
          NonEmpty(
            Map,
            sequencerAlice -> Config("Alice")(factoryAlice),
            sequencerBob -> Config("BobV1")(factoryBob),
          ),
        )
        // Keep Alice, reconfigure Bob, add Carlos
        val config2 = OrderedBucketMergeConfig(
          PositiveInt.tryCreate(2),
          NonEmpty(
            Map,
            sequencerAlice -> Config("Alice")(factoryAlice),
            sequencerBob -> Config("BobV2")(factoryBob),
            sequencerCarlos -> Config("Carlos")(factoryCarlos),
          ),
        )

        val signatureAlice = fakeSignatureFor("Alice")
        val signatureBob = fakeSignatureFor("Bob")
        val signatureCarlos = fakeSignatureFor("Carlos")

        val events = mkEvents(initialCounter, 4)
        val events1 = events.take(2)
        factoryAlice.add(events.map(_.copy(signatures = NonEmpty(Set, signatureAlice))) *)
        factoryBob.add(events1.map(_.copy(signatures = NonEmpty(Set, signatureBob))) *)

        val events2 = events.drop(1)
        factoryBob.add(events2.drop(1).map(_.copy(signatures = NonEmpty(Set, signatureBob))) *)
        factoryCarlos.add(
          events2.take(2).map(_.copy(signatures = NonEmpty(Set, signatureCarlos))) *
        )

        source.offer(config1) shouldBe QueueOfferResult.Enqueued

        sink.request(10)
        sink.expectNext() shouldBe Left(NewConfiguration(config1, initialCounter))
        normalize(sink.expectNext().value) shouldBe normalize(
          Event(
            initialCounter + 1,
            NonEmpty(Set, signatureAlice, signatureBob),
          ).asOrdinarySerializedEvent
        )
        sink.expectNoMessage()
        loggerFactory.assertLogs(
          {
            source.offer(config2) shouldBe QueueOfferResult.Enqueued
            sink.expectNext() shouldBe Left(NewConfiguration(config2, initialCounter + 1))
            val outputs =
              Set(sink.expectNext(), sink.expectNext()).map(_.map(normalize))
            val expected = Set(
              Left(ActiveSourceTerminated(sequencerBob, None)),
              Right(Event(initialCounter + 2, NonEmpty(Set, signatureAlice, signatureCarlos))),
            ).map(_.map(event => normalize(event.asOrdinarySerializedEvent)))
            outputs shouldBe expected
          },
          _.errorMessage should include(ResilientSequencerSubscription.ForkHappened.id),
          _.warningMessage should include(s"Sequencer subscription for $sequencerBob failed with"),
        )
        source.complete()
        sink.expectComplete()
        doneF.futureValue
      }
  }
}

object SequencerAggregatorAkkaTest {
  final case class Config(id: String)(
      override val subscriptionFactory: SequencerSubscriptionFactoryAkka[TestSubscriptionError]
  ) extends HasSequencerSubscriptionFactoryAkka[TestSubscriptionError]
}
