// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import cats.implicits.catsSyntaxOptionId
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{Fingerprint, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.{AtomicHealthComponent, ComponentHealthState}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasRunOnClosing, OnShutdownRunner}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencing.SequencerAggregatorPekko.HasSequencerSubscriptionFactoryPekko
import com.digitalasset.canton.sequencing.SequencerAggregatorPekkoTest.Config
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.client.TestSequencerSubscriptionFactoryPekko.{
  Error,
  Event,
  Failure,
}
import com.digitalasset.canton.sequencing.client.TestSubscriptionError.UnretryableError
import com.digitalasset.canton.topology.{DefaultTestIdentities, SequencerId}
import com.digitalasset.canton.util.OrderedBucketMergeHub.{
  ActiveSourceTerminated,
  DeadlockDetected,
  DeadlockTrigger,
  NewConfiguration,
}
import com.digitalasset.canton.util.{EitherTUtil, OrderedBucketMergeConfig, ResourceUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  ProtocolVersionChecksFixtureAnyWordSpec,
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
    )(env => withFixture(test.toNoArgTest(env)))

  private val synchronizerId = DefaultTestIdentities.synchronizerId

  private def mkAggregatorPekko(
      validator: SequencedEventValidator =
        SequencedEventValidator.noValidation(DefaultTestIdentities.synchronizerId, warn = false)
  )(implicit fixture: FixtureParam): SequencerAggregatorPekko =
    new SequencerAggregatorPekko(
      synchronizerId,
      _ => validator,
      PositiveInt.one,
      fixture.subscriberCryptoApi.pureCrypto,
      loggerFactory,
      enableInvariantCheck = true,
    )

  private def fakeSignatureFor(name: String): Signature =
    SymbolicCrypto.signature(
      ByteString.EMPTY,
      Fingerprint.tryFromString(name),
    )

  // Sort the signatures by the fingerprint of the key to get a deterministic ordering
  private def normalize(event: SequencedSerializedEvent): SequencedSerializedEvent =
    event.copy(signedEvent =
      event.signedEvent.copy(signatures =
        event.signedEvent.signatures.sortBy(_.signedBy.toProtoPrimitive)
      )
    )(event.traceContext)

  private def mkEvents(startingTimestampO: Option[CantonTimestamp], amount: Long): Seq[Event] = {
    val startTimestamp = startingTimestampO.getOrElse(CantonTimestamp.Epoch)
    (0L until amount).map(i => Event(startTimestamp.addMicros(i)))
  }

  private class TestAtomicHealthComponent(override val name: String) extends AtomicHealthComponent {
    override protected def initialHealthState: ComponentHealthState =
      ComponentHealthState.NotInitializedState
    override protected def associatedHasRunOnClosing: HasRunOnClosing =
      new OnShutdownRunner.PureOnShutdownRunner(logger)
    override protected def logger: TracedLogger = SequencerAggregatorPekkoTest.this.logger
  }

  "aggregator" should {
    "pass through events from a single sequencer subscription" in { implicit fixture =>
      import fixture.*

      val aggregator = mkAggregatorPekko()
      val factory = TestSequencerSubscriptionFactoryPekko(loggerFactory)
      factory.add(mkEvents(startingTimestampO = None, 3)*)

      val config = OrderedBucketMergeConfig(
        PositiveInt.one,
        NonEmpty(Map, sequencerAlice -> Config("")(factory)),
      )
      val configSource =
        Source.single(config).concat(Source.never).viaMat(KillSwitches.single)(Keep.right)

      val ((killSwitch, (doneF, _health)), sink) = configSource
        .viaMat(aggregator.aggregateFlow(Left(None)))(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(5)
      sink.expectNext() shouldBe Left(NewConfiguration(config, None))
      sink.expectNext().value shouldBe
        Event(CantonTimestamp.Epoch).asOrdinarySerializedEvent
      sink.expectNext().value shouldBe
        Event(CantonTimestamp.Epoch.addMicros(1L)).asOrdinarySerializedEvent
      sink.expectNext().value shouldBe
        Event(CantonTimestamp.Epoch.addMicros(2L)).asOrdinarySerializedEvent
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
            .viaMat(aggregator.aggregateFlow(Left(None)))(Keep.both)
            .toMat(TestSink.probe)(Keep.both)
            .run()

          sink.request(5)
          sink.expectNext() shouldBe Left(NewConfiguration(config, None))
          sink.expectNext() shouldBe Left(ActiveSourceTerminated(sequencerAlice, None))
          sink.expectNext() shouldBe Left(
            DeadlockDetected(Seq.empty, DeadlockTrigger.ActiveSourceTermination)
          )

          (handle, sink)
        },
        _.warningMessage should include(
          s"Sequencer subscription for $sequencerAlice failed with $UnretryableError"
        ),
        _.errorMessage should include(
          s"Sequencer subscription for synchronizer $synchronizerId is now stuck. Needs operator intervention to reconfigure the sequencer connections."
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
            .viaMat(aggregator.aggregateFlow(Left(None)))(Keep.both)
            .toMat(TestSink.probe)(Keep.both)
            .run()

          sink.request(5)
          sink.expectNext() shouldBe Left(NewConfiguration(config, None))
          sink.expectNext() shouldBe Left(ActiveSourceTerminated(sequencerAlice, Some(ex)))
          sink.expectNext() shouldBe Left(
            DeadlockDetected(Seq.empty, DeadlockTrigger.ActiveSourceTermination)
          )

          (handle, sink)
        },
        _.errorMessage should include(s"Sequencer subscription for $sequencerAlice failed"),
        _.errorMessage should include(
          s"Sequencer subscription for synchronizer $synchronizerId is now stuck. Needs operator intervention to reconfigure the sequencer connections."
        ),
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
        .viaMat(aggregator.aggregateFlow(Left(None)))(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val factory = TestSequencerSubscriptionFactoryPekko(loggerFactory)
      factory.add(mkEvents(startingTimestampO = None, 3)*)
      factory.add(mkEvents(startingTimestampO = Some(CantonTimestamp.Epoch.addMicros(2L)), 3)*)
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
      sink.expectNext() shouldBe Left(NewConfiguration(config1, None))
      sink.expectNext().value shouldBe
        Event(CantonTimestamp.Epoch).asOrdinarySerializedEvent
      sink.expectNext().value shouldBe
        Event(CantonTimestamp.Epoch.addMicros(1L)).asOrdinarySerializedEvent
      sink.expectNext().value shouldBe
        Event(CantonTimestamp.Epoch.addMicros(2)).asOrdinarySerializedEvent
      sink.expectNoMessage()
      source.offer(config2) shouldBe QueueOfferResult.Enqueued
      sink.expectNext() shouldBe Left(
        NewConfiguration(config2, CantonTimestamp.Epoch.addMicros(2L).some)
      )
      sink.expectNext().value shouldBe
        Event(CantonTimestamp.Epoch.addMicros(3)).asOrdinarySerializedEvent
      sink.expectNext().value shouldBe
        Event(CantonTimestamp.Epoch.addMicros(4)).asOrdinarySerializedEvent
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

        val events = mkEvents(startingTimestampO = None, 3)
        factoryAlice.add(events.take(1).map(_.copy(signatures = NonEmpty(Set, signatureAlice)))*)
        factoryBob.add(events.slice(1, 2).map(_.copy(signatures = NonEmpty(Set, signatureBob)))*)
        factoryCarlos.add(events.take(3).map(_.copy(signatures = NonEmpty(Set, signatureCarlos)))*)

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
          .viaMat(aggregator.aggregateFlow(Left(None)))(Keep.both)
          .toMat(TestSink.probe)(Keep.both)
          .run()

        sink.request(4)
        sink.expectNext() shouldBe Left(NewConfiguration(config, None))
        normalize(sink.expectNext().value) shouldBe normalize(
          Event(
            timestamp = CantonTimestamp.Epoch,
            NonEmpty(Set, signatureAlice, signatureCarlos),
          ).asOrdinarySerializedEvent
        )
        normalize(sink.expectNext().value) shouldBe normalize(
          Event(
            timestamp = CantonTimestamp.Epoch.addMicros(1L),
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
        defaultSynchronizerId,
        testedProtocolVersion,
        subscriberCryptoApi,
        loggerFactory,
        timeouts,
      ) {
        override protected def verifySignature(
            priorEventO: Option[ProcessingSerializedEvent],
            event: SequencedSerializedEvent,
            sequencerId: SequencerId,
            protocolVersion: ProtocolVersion,
        ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit] =
          EitherTUtil.unitUS
      }

      val initialTimestamp = CantonTimestamp.Epoch.addMicros(10L)
      val aggregator = mkAggregatorPekko(validator)
      val ((source, (doneF, health_)), sink) = Source
        .queue[OrderedBucketMergeConfig[SequencerId, Config]](1)
        .viaMat(
          aggregator.aggregateFlow(Right(Event(initialTimestamp).asOrdinarySerializedEvent))
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

      val events = mkEvents(Some(initialTimestamp), 4)
      val events1 = events.take(2)
      // alice reports events 10,11,12,13
      factoryAlice.add(events.map(_.copy(signatures = NonEmpty(Set, signatureAlice)))*)
      // bob reports events 10,11
      factoryBob.add(events1.map(_.copy(signatures = NonEmpty(Set, signatureBob)))*)

      // events
      val events2 = events.drop(1)
      // bob reports events 12,13
      factoryBob.add(events2.drop(1).map(_.copy(signatures = NonEmpty(Set, signatureBob)))*)
      // carlos reports events 11,12
      factoryCarlos.add(
        events2.take(2).map(_.copy(signatures = NonEmpty(Set, signatureCarlos)))*
      )

      source.offer(config1) shouldBe QueueOfferResult.Enqueued

      sink.request(10)
      sink.expectNext() shouldBe Left(NewConfiguration(config1, initialTimestamp.some))
      normalize(sink.expectNext().value) shouldBe normalize(
        Event(
          initialTimestamp.addMicros(1L),
          NonEmpty(Set, signatureAlice, signatureBob),
        ).asOrdinarySerializedEvent
      )
      sink.expectNoMessage()
      loggerFactory.assertLogs(
        {
          source.offer(config2) shouldBe QueueOfferResult.Enqueued
          sink.expectNext() shouldBe Left(
            NewConfiguration(config2, initialTimestamp.addMicros(1L).some)
          )
          val outputs =
            Set(sink.expectNext(), sink.expectNext()).map(_.map(normalize))
          val expected = Set(
            Left(ActiveSourceTerminated(sequencerBob, None)),
            Right(
              Event(initialTimestamp.addMicros(2L), NonEmpty(Set, signatureAlice, signatureCarlos))
            ),
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

      factory.add((0L to 2L).map(offset => Event(CantonTimestamp.Epoch.addMicros(offset)))*)
      factory.add(Error(UnretryableError))
      val config = OrderedBucketMergeConfig(
        PositiveInt.one,
        NonEmpty(Map, sequencerAlice -> Config("")(factory)),
      )
      val configSource =
        Source.single(config).concat(Source.never).viaMat(KillSwitches.single)(Keep.right)

      val ((killSwitch, (doneF, reportedHealth)), sink) = configSource
        .viaMat(aggregator.aggregateFlow(Left(None)))(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      reportedHealth.getState shouldBe ComponentHealthState.NotInitializedState

      sink.request(10)
      health.resolveUnhealthy()
      sink.expectNext() shouldBe Left(NewConfiguration(config, None))
      sink.expectNext() shouldBe Right(Event(CantonTimestamp.Epoch).asOrdinarySerializedEvent)

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
        Event(CantonTimestamp.Epoch.addMicros(1L)).asOrdinarySerializedEvent
      )

      health.resolveUnhealthy()
      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.Ok()
      }

      killSwitch.shutdown()
      doneF.futureValue
    }

    "aggregate health signal for multiple sequencers" in { implicit fixture =>
      import fixture.*

      val aggregator = mkAggregatorPekko()
      val healthAlice = new TestAtomicHealthComponent("health-signal-alice")
      val healthBob = new TestAtomicHealthComponent("health-signal-bob")
      val healthCarlos = new TestAtomicHealthComponent("health-signal-carlos")

      val factoryAlice = new TestSequencerSubscriptionFactoryPekko(healthAlice, loggerFactory)
      val factoryBob = new TestSequencerSubscriptionFactoryPekko(healthBob, loggerFactory)
      val factoryCarlos = new TestSequencerSubscriptionFactoryPekko(healthCarlos, loggerFactory)

      factoryAlice.add((0L to 2L).map(offset => Event(CantonTimestamp.Epoch.addMicros(offset)))*)
      factoryBob.add((0L to 2L).map(offset => Event(CantonTimestamp.Epoch.addMicros(offset)))*)
      factoryCarlos.add((0L to 2L).map(offset => Event(CantonTimestamp.Epoch.addMicros(offset)))*)

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
        .viaMat(aggregator.aggregateFlow(Left(None)))(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      reportedHealth.getState shouldBe ComponentHealthState.NotInitializedState

      sink.request(10)
      Seq(healthAlice, healthBob, healthCarlos).foreach(_.resolveUnhealthy())

      sink.expectNext() shouldBe Left(NewConfiguration(config, None))

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
          s"Disconnected from synchronizer $synchronizerId"
        )
      }
    }

    "become unhealthy upon deadlock notifications" in { implicit fixture =>
      import fixture.*

      val aggregator = mkAggregatorPekko()

      val factoryAlice1 =
        TestSequencerSubscriptionFactoryPekko(loggerFactory.append("factory", "alice-1"))
      val factoryBob1 =
        TestSequencerSubscriptionFactoryPekko(loggerFactory.append("factory", "bob-1"))
      val factoryCarlos =
        TestSequencerSubscriptionFactoryPekko(loggerFactory.append("factory", "carlos"))

      val signatureAlice = fakeSignatureFor("Alice")
      val signatureBob = fakeSignatureFor("Bob")
      val signatureCarlos = fakeSignatureFor("Carlos")

      factoryAlice1.add(
        mkEvents(startingTimestampO = None, 1)
          .map(_.copy(signatures = NonEmpty(Set, signatureAlice)))*
      )
      factoryBob1.add(
        mkEvents(startingTimestampO = None, 1)
          .map(_.copy(signatures = NonEmpty(Set, signatureBob)))*
      )
      factoryCarlos.add(
        mkEvents(startingTimestampO = Some(CantonTimestamp.Epoch.addMicros(3L)), 1)
          .map(_.copy(signatures = NonEmpty(Set, signatureCarlos)))*
      )

      val config1 = OrderedBucketMergeConfig(
        PositiveInt.tryCreate(2),
        NonEmpty(
          Map,
          sequencerAlice -> Config("1")(factoryAlice1),
          sequencerBob -> Config("1")(factoryBob1),
          sequencerCarlos -> Config("1")(factoryCarlos),
        ),
      )

      val ((configSource, (doneF, reportedHealth)), sink) = Source
        .queue[OrderedBucketMergeConfig[SequencerId, Config]](1)
        .viaMat(aggregator.aggregateFlow(Left(None)))(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()
      configSource.offer(config1) shouldBe QueueOfferResult.Enqueued

      reportedHealth.getState shouldBe ComponentHealthState.NotInitializedState

      sink.request(10)
      sink.expectNext() shouldBe Left(NewConfiguration(config1, None))
      normalize(sink.expectNext().value) shouldBe
        normalize(
          Event(
            CantonTimestamp.Epoch,
            NonEmpty(Set, signatureAlice, signatureBob),
          ).asOrdinarySerializedEvent
        )

      eventually() {
        reportedHealth.getState shouldBe ComponentHealthState.Ok()
      }

      // Now reconfigure Alice and Bob to get into deadlock: each source provides a different next sequencer counter
      clue("Create a deadlock") {
        val factoryAlice2 =
          TestSequencerSubscriptionFactoryPekko(loggerFactory.append("factory", "alice-2"))
        val factoryBob2 =
          TestSequencerSubscriptionFactoryPekko(loggerFactory.append("factory", "bob-2"))

        factoryAlice2.add(
          mkEvents(startingTimestampO = Some(CantonTimestamp.Epoch.addMicros(1L)), 1)*
        )
        factoryBob2.add(
          mkEvents(startingTimestampO = Some(CantonTimestamp.Epoch.addMicros(2L)), 1)*
        )
        val config2 = OrderedBucketMergeConfig(
          PositiveInt.tryCreate(2),
          NonEmpty(
            Map,
            sequencerAlice -> Config("2")(factoryAlice2),
            sequencerBob -> Config("2")(factoryBob2),
            sequencerCarlos -> Config("1")(factoryCarlos),
          ),
        )
        loggerFactory.assertLogs(
          {
            configSource.offer(config2)
            sink.expectNext() shouldBe Left(NewConfiguration(config2, CantonTimestamp.Epoch.some))
            inside(sink.expectNext()) {
              case Left(DeadlockDetected(elems, DeadlockTrigger.ElementBucketing)) =>
                elems should have size 3
            }
            eventually() {
              reportedHealth.getState shouldBe ComponentHealthState.failed(
                s"Sequencer subscriptions have diverged and cannot reach the threshold 2 for synchronizer $synchronizerId any more."
              )
            }
          },
          _.errorMessage should include(
            s"Sequencer subscriptions have diverged and cannot reach the threshold for synchronizer $synchronizerId any more."
          ),
        )
      }

      clue("Resolve deadlock through reconfiguration") {
        val config3 = OrderedBucketMergeConfig(
          PositiveInt.tryCreate(1),
          NonEmpty(
            Map,
            sequencerCarlos -> Config("1")(factoryCarlos),
          ),
        )
        configSource.offer(config3) shouldBe QueueOfferResult.Enqueued
        sink.expectNext() shouldBe Left(NewConfiguration(config3, CantonTimestamp.Epoch.some))
        sink.expectNext().value shouldBe
          Event(
            CantonTimestamp.Epoch.addMicros(3L),
            NonEmpty(Set, signatureCarlos),
          ).asOrdinarySerializedEvent

        eventually() {
          reportedHealth.getState shouldBe ComponentHealthState.Ok()
        }
      }

      configSource.complete()
      sink.expectComplete()
      doneF.futureValue

    }
  }
}

object SequencerAggregatorPekkoTest {
  final case class Config(id: String)(
      override val subscriptionFactory: SequencerSubscriptionFactoryPekko[TestSubscriptionError]
  ) extends HasSequencerSubscriptionFactoryPekko[TestSubscriptionError]
}
