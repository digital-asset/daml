// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{Fingerprint, Signature}
import com.digitalasset.canton.health.{AtomicHealthComponent, ComponentHealthState}
import com.digitalasset.canton.lifecycle.OnShutdownRunner
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencing.SequencerAggregatorPekko.HasSequencerSubscriptionFactoryPekko
import com.digitalasset.canton.sequencing.SequencerAggregatorPekkoTest.Config
import com.digitalasset.canton.sequencing.client.TestSequencerSubscriptionFactoryPekko.{
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
  SequencerSubscriptionFactoryPekko,
  TestSequencerSubscriptionFactoryPekko,
  TestSubscriptionError,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, SequencerId}
import com.digitalasset.canton.util.OrderedBucketMergeHub.{ActiveSourceTerminated, NewConfiguration}
import com.digitalasset.canton.util.{OrderedBucketMergeConfig, ResourceUtil}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  ProtocolVersionChecksFixtureAnyWordSpec,
  SequencerCounter,
}
import com.google.protobuf.ByteString
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.apache.pekko.stream.{KillSwitches, QueueOfferResult}
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.concurrent.duration.DurationInt

class SequencerAggregatorPekkoTest
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

  private val domainId = DefaultTestIdentities.domainId

  private def mkAggregatorPekko(
      validator: SequencedEventValidator =
        SequencedEventValidator.noValidation(DefaultTestIdentities.domainId, warn = false)
  )(implicit fixture: FixtureParam): SequencerAggregatorPekko =
    new SequencerAggregatorPekko(
      domainId,
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

  private class TestAtomicHealthComponent(override val name: String) extends AtomicHealthComponent {
    override protected def initialHealthState: ComponentHealthState =
      ComponentHealthState.NotInitializedState
    override protected def associatedOnShutdownRunner: OnShutdownRunner =
      new OnShutdownRunner.PureOnShutdownRunner(logger)
    override protected def logger: TracedLogger = SequencerAggregatorPekkoTest.this.logger
  }

  "aggregator" should {
    "pass through events from a single sequencer subscription" in { implicit fixture =>
      import fixture.*

      val aggregator = mkAggregatorPekko()
      val factory = TestSequencerSubscriptionFactoryPekko(loggerFactory)
      factory.add(mkEvents(SequencerCounter.Genesis, 3) *)

      val config = OrderedBucketMergeConfig(
        PositiveInt.one,
        NonEmpty(Map, sequencerAlice -> Config("")(factory)),
      )
      val configSource =
        Source.single(config).concat(Source.never).viaMat(KillSwitches.single)(Keep.right)

      val ((killSwitch, (doneF, _health)), sink) = configSource
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
      val aggregator = mkAggregatorPekko()
      val factory = TestSequencerSubscriptionFactoryPekko(loggerFactory)
      factory.add(Error(UnretryableError))
      val config = OrderedBucketMergeConfig(
        PositiveInt.one,
        NonEmpty(Map, sequencerAlice -> Config("")(factory)),
      )
      val configSource =
        Source.single(config).concat(Source.never).viaMat(KillSwitches.single)(Keep.right)

      val ((killSwitch, (doneF, _health)), sink) = loggerFactory.assertLogs(
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
      val aggregator = mkAggregatorPekko()
      val factory = TestSequencerSubscriptionFactoryPekko(loggerFactory)
      val ex = new Exception("Alice subscription failure")
      factory.add(Failure(ex))
      val config = OrderedBucketMergeConfig(
        PositiveInt.one,
        NonEmpty(Map, sequencerAlice -> Config("")(factory)),
      )
      val configSource =
        Source.single(config).concat(Source.never).viaMat(KillSwitches.single)(Keep.right)

      val ((killSwitch, (doneF, _health)), sink) = loggerFactory.assertLogs(
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

      val aggregator = mkAggregatorPekko()
      val ((source, (doneF, _health)), sink) = Source
        .queue[OrderedBucketMergeConfig[SequencerId, Config]](1)
        .viaMat(aggregator.aggregateFlow(Left(SequencerCounter.Genesis)))(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val factory = TestSequencerSubscriptionFactoryPekko(loggerFactory)
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

    "pass through the event only if sufficiently many sequencer IDs send it" in {
      implicit fixture =>
        import fixture.*

        val aggregator = mkAggregatorPekko()

        val factoryAlice = TestSequencerSubscriptionFactoryPekko(loggerFactory)
        val factoryBob = TestSequencerSubscriptionFactoryPekko(loggerFactory)
        val factoryCarlos = TestSequencerSubscriptionFactoryPekko(loggerFactory)

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

        val ((killSwitch, (doneF, _health)), sink) = configSource
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

    "support reconfiguring the threshold and sequencers" in { implicit fixture =>
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
      val aggregator = mkAggregatorPekko(validator)
      val ((source, (doneF, health_)), sink) = Source
        .queue[OrderedBucketMergeConfig[SequencerId, Config]](1)
        .viaMat(
          aggregator.aggregateFlow(Right(Event(initialCounter).asOrdinarySerializedEvent))
        )(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val factoryAlice = TestSequencerSubscriptionFactoryPekko(loggerFactory)
      val factoryBob = TestSequencerSubscriptionFactoryPekko(loggerFactory)
      val factoryCarlos = TestSequencerSubscriptionFactoryPekko(loggerFactory)

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
      sink.expectNext() shouldBe Left(NewConfiguration(config1, initialCounter - 1L))
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

    "forward health signal for a single sequencer" in { implicit fixture =>
      import fixture.*

      val aggregator = mkAggregatorPekko()
      val health = new TestAtomicHealthComponent("forward-health-signal-test")
      val factory = new TestSequencerSubscriptionFactoryPekko(health, loggerFactory)
      factory.add((0 to 2).map(sc => Event(SequencerCounter(sc))) *)
      factory.add(Error(UnretryableError))
      val config = OrderedBucketMergeConfig(
        PositiveInt.one,
        NonEmpty(Map, sequencerAlice -> Config("")(factory)),
      )
      val configSource =
        Source.single(config).concat(Source.never).viaMat(KillSwitches.single)(Keep.right)

      val ((killSwitch, (doneF, reportedHealth)), sink) = configSource
        .viaMat(aggregator.aggregateFlow(Left(SequencerCounter.Genesis)))(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      reportedHealth.getState shouldBe ComponentHealthState.NotInitializedState

      sink.request(10)
      health.resolveUnhealthy()
      sink.expectNext() shouldBe Left(NewConfiguration(config, SequencerCounter.Genesis - 1L))
      sink.expectNext() shouldBe Right(Event(SequencerCounter.Genesis).asOrdinarySerializedEvent)

      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.Ok()
      }

      health.degradationOccurred("some degradation")
      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.degraded("some degradation")
      }

      health.failureOccurred("some failure")
      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.failed("some failure")
      }

      sink.expectNext() shouldBe Right(
        Event(SequencerCounter.Genesis + 1).asOrdinarySerializedEvent
      )

      health.resolveUnhealthy()
      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.Ok()
      }

      killSwitch.shutdown()
      doneF.futureValue
    }

    "aggregate health signal for a multiple sequencers" in { implicit fixture =>
      import fixture.*

      val aggregator = mkAggregatorPekko()
      val healthAlice = new TestAtomicHealthComponent("health-signal-alice")
      val healthBob = new TestAtomicHealthComponent("health-signal-bob")
      val healthCarlos = new TestAtomicHealthComponent("health-signal-carlos")

      val factoryAlice = new TestSequencerSubscriptionFactoryPekko(healthAlice, loggerFactory)
      val factoryBob = new TestSequencerSubscriptionFactoryPekko(healthBob, loggerFactory)
      val factoryCarlos = new TestSequencerSubscriptionFactoryPekko(healthCarlos, loggerFactory)

      factoryAlice.add((0 to 2).map(sc => Event(SequencerCounter(sc))) *)
      factoryBob.add((0 to 2).map(sc => Event(SequencerCounter(sc))) *)
      factoryCarlos.add((0 to 2).map(sc => Event(SequencerCounter(sc))) *)

      val config = OrderedBucketMergeConfig(
        PositiveInt.tryCreate(2),
        NonEmpty(
          Map,
          sequencerAlice -> Config("")(factoryAlice),
          sequencerBob -> Config("")(factoryBob),
          sequencerCarlos -> Config("")(factoryCarlos),
        ),
      )
      val configSource =
        Source.single(config).concat(Source.never).viaMat(KillSwitches.single)(Keep.right)

      val ((killSwitch, (doneF, reportedHealth)), sink) = configSource
        .viaMat(aggregator.aggregateFlow(Left(SequencerCounter.Genesis)))(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      reportedHealth.getState shouldBe ComponentHealthState.NotInitializedState

      sink.request(10)
      Seq(healthAlice, healthBob, healthCarlos).foreach(_.resolveUnhealthy())

      sink.expectNext() shouldBe Left(NewConfiguration(config, SequencerCounter.Genesis - 1L))

      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.Ok()
      }

      healthAlice.failureOccurred("Alice failed")
      // We still have threshold many sequencers that are healthy, so the subscription is healthy overall
      always(durationOfSuccess = 100.milliseconds) {
        reportedHealth.getState shouldBe ComponentHealthState.Ok()
      }

      healthBob.degradationOccurred("Bob degraded")
      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.degraded(
          s"Failed sequencer subscriptions for [$sequencerAlice]. Degraded sequencer subscriptions for [$sequencerBob]."
        )
      }

      healthAlice.resolveUnhealthy()
      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.Ok()
      }

      healthAlice.degradationOccurred("Alice degraded")
      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.degraded(
          s"Degraded sequencer subscriptions for [$sequencerBob, $sequencerAlice]."
        )
      }

      healthBob.failureOccurred("Bob failed")
      healthCarlos.failureOccurred("Carlos failed")
      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.failed(
          s"Failed sequencer subscriptions for [$sequencerBob, $sequencerCarlos]. Degraded sequencer subscriptions for [$sequencerAlice]."
        )
      }

      healthAlice.resolveUnhealthy()
      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.failed(
          s"Failed sequencer subscriptions for [$sequencerBob, $sequencerCarlos]."
        )
      }

      killSwitch.shutdown()
      doneF.futureValue

      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.failed(
          s"Disconnected from domain $domainId"
        )
      }
    }
  }
}

object SequencerAggregatorPekkoTest {
  final case class Config(id: String)(
      override val subscriptionFactory: SequencerSubscriptionFactoryPekko[TestSubscriptionError]
  ) extends HasSequencerSubscriptionFactoryPekko[TestSubscriptionError]
}
