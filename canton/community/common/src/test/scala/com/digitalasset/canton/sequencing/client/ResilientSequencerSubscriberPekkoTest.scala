// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.HealthComponent.AlwaysHealthyComponent
import com.digitalasset.canton.health.{ComponentHealthState, HealthComponent}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import com.digitalasset.canton.sequencing.client.TestSubscriptionError.{
  FatalExn,
  RetryableError,
  RetryableExn,
  UnretryableError,
}
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, SignedContent}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.{DefaultTestIdentities, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.testkit.StreamSpec
import org.apache.pekko.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.pekko.stream.testkit.scaladsl.TestSink

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.{Deadline, DurationInt, FiniteDuration}

class ResilientSequencerSubscriberPekkoTest extends StreamSpec with BaseTest {
  import TestSequencerSubscriptionFactoryPekko.*

  // Override the implicit from PekkoSpec so that we don't get ambiguous implicits
  override val patience: PatienceConfig = defaultPatience

  // very short to speedup test
  private val InitialDelay: FiniteDuration = 1.millisecond
  private val MaxDelay: FiniteDuration =
    1025.millis // 1 + power of 2 because InitialDelay keeps being doubled

  private def retryDelay(maxDelay: FiniteDuration = MaxDelay) =
    SubscriptionRetryDelayRule(InitialDelay, maxDelay, maxDelay)

  private def createResilientSubscriber[E](
      subscriptionFactory: SequencerSubscriptionFactoryPekko[E],
      retryDelayRule: SubscriptionRetryDelayRule = retryDelay(),
  ): ResilientSequencerSubscriberPekko[E] = {
    new ResilientSequencerSubscriberPekko[E](
      retryDelayRule,
      subscriptionFactory,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
  }

  "ResilientSequencerSubscriberPekko" should {
    "not retry on an unrecoverable error" in assertAllStagesStopped {
      val factory = TestSequencerSubscriptionFactoryPekko(
        loggerFactory.appendUnnamedKey("case", "unrecoverable-error")
      )
      val subscriber = createResilientSubscriber(factory)
      factory.add(Error(UnretryableError))
      val subscription = subscriber.subscribeFrom(SequencerCounter.Genesis)
      loggerFactory.assertLogs(
        subscription.source.toMat(Sink.ignore)(Keep.right).run().futureValue,
        _.warningMessage should include(
          s"Closing resilient sequencer subscription due to error: $UnretryableError"
        ),
      )
      // Health updates are asynchronous
      eventually() {
        subscription.health.getState shouldBe subscription.health.closingState
      }
    }

    "retry on recoverable errors" in assertAllStagesStopped {
      val factory = TestSequencerSubscriptionFactoryPekko(
        loggerFactory.appendUnnamedKey("case", "retry-on-error")
      )
      val subscriber = createResilientSubscriber(factory)
      factory.add(Error(RetryableError))
      factory.add(Error(RetryableError))
      factory.add(Error(UnretryableError))
      val subscription = subscriber.subscribeFrom(SequencerCounter.Genesis)
      loggerFactory.assertLogs(
        subscription.source
          .toMat(Sink.ignore)(Keep.right)
          .run()
          .futureValue,
        _.warningMessage should include(
          s"Closing resilient sequencer subscription due to error: $UnretryableError"
        ),
      )
      // Health updates are asynchronous
      eventually() {
        subscription.health.getState shouldBe subscription.health.closingState
      }
    }

    "retry on exceptions until one is fatal" in {
      val factory = TestSequencerSubscriptionFactoryPekko(
        loggerFactory.appendUnnamedKey("case", "retry-on-exception")
      )
      val subscriber = createResilientSubscriber(factory)
      factory.add(Failure(RetryableExn))
      factory.add(Failure(FatalExn))
      val subscription = subscriber.subscribeFrom(SequencerCounter.Genesis)
      loggerFactory.assertLogs(
        subscription.source
          .toMat(Sink.ignore)(Keep.right)
          .run()
          .futureValue,
        _.warningMessage should include(
          "The sequencer subscription encountered an exception and will be restarted"
        ),
        _.errorMessage should include("Closing resilient sequencer subscription due to exception"),
      )
      // Health updates are asynchronous
      eventually() {
        subscription.health.getState shouldBe subscription.health.closingState
      }
    }

    "restart from last received counter" in {
      val factory = TestSequencerSubscriptionFactoryPekko(
        loggerFactory.appendUnnamedKey("case", "restart-from-counter")
      )
      val subscriber = createResilientSubscriber(factory)
      factory.subscribe(start =>
        (start to (start + 10)).map(sc => Event(sc)) :+ Error(RetryableError)
      )
      factory.subscribe(start => (start to (start + 10)).map(sc => Event(sc)))

      val ((killSwitch, doneF), sink) =
        subscriber
          .subscribeFrom(SequencerCounter.Genesis)
          .source
          .map(_.value)
          .toMat(TestSink.probe)(Keep.both)
          .run()
      sink.request(30)
      val expectedCounters =
        (SequencerCounter.Genesis to SequencerCounter(10)) ++
          (SequencerCounter(10) to SequencerCounter(20))
      expectedCounters.zipWithIndex.foreach { case (sc, i) =>
        clue(s"Output stream element $i") {
          sink.expectNext(Right(mkOrdinarySerializedEvent(sc)))
        }
      }
      killSwitch.shutdown()
      doneF.futureValue
    }

    "correctly indicates whether we've received items when calculating the next retry delay" in assertAllStagesStopped {
      val hasReceivedEventsCalls = new AtomicReference[Seq[Boolean]](Seq.empty)
      val captureHasEvent = new SubscriptionRetryDelayRule {
        override def nextDelay(
            previousDelay: FiniteDuration,
            hasReceivedEvent: Boolean,
        ): FiniteDuration = {
          hasReceivedEventsCalls.getAndUpdate(_ :+ hasReceivedEvent)
          1.milli
        }

        override val initialDelay: FiniteDuration = 1.milli
        override val warnDelayDuration: FiniteDuration = 100.millis
      }
      val factory = TestSequencerSubscriptionFactoryPekko(
        loggerFactory.appendUnnamedKey("case", "calculate-retry-delay")
      )
      val subscriber = createResilientSubscriber(factory, captureHasEvent)

      // provide an event then close with a recoverable error
      factory.add(Event(SequencerCounter(1L)), Error(RetryableError))
      // don't provide an event and close immediately with a recoverable error
      factory.add(Error(RetryableError))
      // don't provide an event and close immediately
      factory.add(Complete)

      subscriber
        .subscribeFrom(SequencerCounter.Genesis)
        .source
        .toMat(Sink.ignore)(Keep.right)
        .run()
        .futureValue

      // An unretryable completion does not call the retry delay rule, so there should be only two calls recorded
      hasReceivedEventsCalls.get() shouldBe Seq(true, false)
    }

    "retry until closing if the sequencer is permanently unavailable" in assertAllStagesStopped {
      val maxDelay = 100.milliseconds

      val factory = TestSequencerSubscriptionFactoryPekko(
        loggerFactory.appendUnnamedKey("case", "retry-until-closing")
      )
      val subscriber = createResilientSubscriber(factory, retryDelay(maxDelay))
      // Always close with RetryableError
      for (_ <- 1 to 100) {
        factory.add(Error(RetryableError))
      }

      val startTime = Deadline.now
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val subscription = subscriber.subscribeFrom(SequencerCounter.Genesis)
          // Initially, everything looks healthy
          subscription.health.isFailed shouldBe false

          val (killSwitch, doneF) = subscription.source.toMat(Sink.ignore)(Keep.left).run()
          // we retry until we become unhealthy
          eventually(maxPollInterval = 10.milliseconds) {
            subscription.health.isFailed shouldBe true
          }

          // Check that it has hit MaxDelay. We can't really check an upper bound as it would make the test flaky
          -startTime.timeLeft should be >= maxDelay

          killSwitch.shutdown()
          doneF.futureValue

          eventually() {
            subscription.health.getState shouldBe subscription.health.closingState
          }
        },
        logEntries => {
          logEntries should not be empty
          forEvery(logEntries) {
            _.warningMessage should (include(s"Waiting $maxDelay before reconnecting") or include(
              LostSequencerSubscription.id
            ))
          }
        },
      )
    }

    "return to healthy when messages are received again" in assertAllStagesStopped {
      val maxDelay = 100.milliseconds

      val factory = TestSequencerSubscriptionFactoryPekko(
        loggerFactory.appendUnnamedKey("case", "return-to-healthy")
      )
      val subscriber = createResilientSubscriber(factory, retryDelay(maxDelay))
      // retryDelay doubles the delay upon each attempt until it hits `maxDelay`,
      // so we set it to two more such that we get the chance to see the unhealthy state
      val retries = (Math.log(maxDelay.toMillis.toDouble) / Math.log(2.0d)).ceil.toInt + 2
      for (_ <- 1 to retries) {
        factory.add(Error(RetryableError))
      }
      factory.add((1 to 10).map(sc => Event(SequencerCounter(sc.toLong)))*)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val subscription = subscriber
            .subscribeFrom(SequencerCounter.Genesis)

          // Initially, everything looks healthy
          subscription.health.isFailed shouldBe false

          val (killSwitch, doneF) = subscription.source.toMat(Sink.ignore)(Keep.left).run()

          logger.debug("Wait until the subscription becomes unhealthy")
          eventually(maxPollInterval = 10.milliseconds) {
            subscription.health.isFailed shouldBe true
          }

          // The factory should eventually produce new elements. So we should return to healthy
          logger.debug("Wait until the subscription becomes healthy again")
          eventually(maxPollInterval = 10.milliseconds) {
            subscription.health.getState shouldBe ComponentHealthState.Ok()
          }

          killSwitch.shutdown()
          doneF.futureValue
        },
        logEntries => {
          logEntries should not be empty
          forEvery(logEntries) {
            _.warningMessage should (include(s"Waiting $maxDelay before reconnecting") or include(
              LostSequencerSubscription.id
            ))
          }
        },
      )
    }
  }
}

class TestSequencerSubscriptionFactoryPekko(
    health: HealthComponent,
    override protected val loggerFactory: NamedLoggerFactory,
) extends SequencerSubscriptionFactoryPekko[TestSubscriptionError]
    with NamedLogging {
  import TestSequencerSubscriptionFactoryPekko.*

  override def sequencerId: SequencerId = DefaultTestIdentities.sequencerId

  private val sources = new AtomicReference[Seq[SequencerCounter => Seq[Element]]](Seq.empty)

  def add(next: Element*): Unit = subscribe(_ => next)

  def subscribe(subscribe: SequencerCounter => Seq[Element]): Unit =
    sources.getAndUpdate(_ :+ subscribe).discard

  override def create(startingCounter: SequencerCounter)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionPekko[TestSubscriptionError] = {
    val srcs = sources.getAndUpdate(_.drop(1))
    val subscribe = srcs.headOption.getOrElse(
      throw new IllegalStateException(
        "Requesting more resubscriptions than provided by the test setup"
      )
    )

    logger.debug(s"Creating SequencerSubscriptionPekko at starting counter $startingCounter")

    val source = Source(subscribe(startingCounter))
      // Add an incomplete unproductive source at the end to prevent automatic completion signals
      .concat(Source.never[Element])
      .withUniqueKillSwitchMat()(Keep.right)
      .mapConcat { withKillSwitch =>
        noTracingLogger.debug(s"Processing element ${withKillSwitch.value}")
        withKillSwitch.traverse {
          case Error(error) =>
            withKillSwitch.killSwitch.shutdown()
            Seq(Left(error))
          case Complete =>
            withKillSwitch.killSwitch.shutdown()
            Seq.empty
          case event: Event => Seq(Right(event.asOrdinarySerializedEvent))
          case Failure(ex) => throw ex
        }
      }
      .takeUntilThenDrain(_.isLeft)
      .watchTermination()(Keep.both)

    SequencerSubscriptionPekko[TestSubscriptionError](source, health)
  }

  override val retryPolicy: SubscriptionErrorRetryPolicyPekko[TestSubscriptionError] =
    new TestSubscriptionErrorRetryPolicyPekko
}

object TestSequencerSubscriptionFactoryPekko {
  def apply(loggerFactory: NamedLoggerFactory): TestSequencerSubscriptionFactoryPekko = {
    val alwaysHealthyComponent = new AlwaysHealthyComponent(
      "TestSequencerSubscriptionFactory",
      loggerFactory.getTracedLogger(classOf[TestSequencerSubscriptionFactoryPekko]),
    )
    new TestSequencerSubscriptionFactoryPekko(alwaysHealthyComponent, loggerFactory)
  }

  sealed trait Element extends Product with Serializable

  final case class Error(error: TestSubscriptionError) extends Element
  final case class Failure(exception: Exception) extends Element
  case object Complete extends Element
  final case class Event(
      counter: SequencerCounter,
      signatures: NonEmpty[Set[Signature]] = Signature.noSignatures,
  ) extends Element {
    def asOrdinarySerializedEvent: OrdinarySerializedEvent =
      mkOrdinarySerializedEvent(counter, signatures)
  }

  def mkOrdinarySerializedEvent(
      counter: SequencerCounter,
      signatures: NonEmpty[Set[Signature]] = Signature.noSignatures,
  ): OrdinarySerializedEvent = {
    val sequencedEvent = Deliver.create(
      counter,
      CantonTimestamp.Epoch.addMicros(counter.unwrap),
      DefaultTestIdentities.domainId,
      None,
      Batch.empty(BaseTest.testedProtocolVersion),
      BaseTest.testedProtocolVersion,
    )
    val signedContent =
      SignedContent.tryCreate(
        sequencedEvent,
        signatures.toSeq,
        None,
        SignedContent.protocolVersionRepresentativeFor(BaseTest.testedProtocolVersion),
      )
    OrdinarySequencedEvent(signedContent, None)(TraceContext.empty)
  }

  private class TestSubscriptionErrorRetryPolicyPekko
      extends SubscriptionErrorRetryPolicyPekko[TestSubscriptionError] {
    override def retryOnError(subscriptionError: TestSubscriptionError, receivedItems: Boolean)(
        implicit loggingContext: ErrorLoggingContext
    ): Boolean = {
      subscriptionError match {
        case RetryableError => true
        case UnretryableError => false
      }
    }

    override def retryOnException(ex: Throwable)(implicit
        loggingContext: ErrorLoggingContext
    ): Boolean = ex match {
      case RetryableExn => true
      case _ => false
    }
  }
}
