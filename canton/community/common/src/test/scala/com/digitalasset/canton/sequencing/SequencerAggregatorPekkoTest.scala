// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
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
  SequencedEventTestFixture,
  SequencedEventValidator,
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
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.apache.pekko.stream.{KillSwitches, QueueOfferResult}
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

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
  }
}

object SequencerAggregatorPekkoTest {
  final case class Config(id: String)(
      override val subscriptionFactory: SequencerSubscriptionFactoryPekko[TestSubscriptionError]
  ) extends HasSequencerSubscriptionFactoryPekko[TestSubscriptionError]
}
