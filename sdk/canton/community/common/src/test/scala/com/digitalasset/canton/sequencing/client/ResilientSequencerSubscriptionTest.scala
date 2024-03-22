// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.SubscriptionError
import com.digitalasset.canton.sequencing.client.TestSubscriptionError.{
  FatalExn,
  RetryableError,
  RetryableExn,
  UnretryableError,
}
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, SequencedEvent, SignedContent}
import com.digitalasset.canton.sequencing.{SequencerTestUtils, SerializedEventHandler}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, DiscardOps, HasExecutionContext, SequencerCounter}
import org.scalatest.Assertion
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, *}
import scala.concurrent.{Future, Promise}

sealed trait TestSubscriptionError
    extends SubscriptionError
    with Product
    with Serializable
    with PrettyPrinting {
  override def pretty: Pretty[this.type] = prettyOfObject[this.type]
}
object TestSubscriptionError {
  case object RetryableError extends TestSubscriptionError
  case object UnretryableError extends TestSubscriptionError
  case object RetryableExn extends Exception
  case object FatalExn extends Exception

  val retryRule: CheckedSubscriptionErrorRetryPolicy[TestSubscriptionError] =
    new CheckedSubscriptionErrorRetryPolicy[TestSubscriptionError] {
      override protected def retryInternal(error: TestSubscriptionError, receivedItems: Boolean)(
          implicit traceContext: TraceContext
      ): Boolean = error match {
        case RetryableError => true
        case UnretryableError => false
      }

      override def retryOnException(exn: Throwable, Logger: TracedLogger)(implicit
          traceContext: TraceContext
      ): Boolean =
        exn match {
          case RetryableExn => true
          case _ => false
        }
    }
}

final case class TestHandlerError(message: String)

class ResilientSequencerSubscriptionTest
    extends AsyncWordSpec
    with BaseTest
    with ResilientSequencerSubscriptionTestUtils
    with HasExecutionContext {

  private lazy val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain1::test"))

  "ResilientSequencerSubscription" should {
    "not retry on an unrecoverable error" in {
      val testSubscriptions = SubscriptionTestFactory.mocked.addUnrecoverable()
      val subscription = createSubscription(testSubscriptions)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          subscription.start

          for {
            reason <- subscription.closeReason
          } yield reason should be(UnretryableError)
        },
        _.map(
          _.warningMessage
        ) should contain only "Closing resilient sequencer subscription due to error: UnretryableError",
      )
    }

    "retry on recoverable errors" in {
      val testSubscriptions = SubscriptionTestFactory.mocked
        .addRecoverable()
        .addRecoverable()
        .addUnrecoverable()
      val subscription = createSubscription(testSubscriptions)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          subscription.start

          for {
            closeReason <- subscription.closeReason
            _ = closeReason should be(UnretryableError)
          } yield testSubscriptions.allShouldHaveBeenUsed
        },
        _.map(
          _.warningMessage
        ) should contain only "Closing resilient sequencer subscription due to error: UnretryableError",
      )
    }

    "retry on exceptions until one is fatal" in {
      val testSubscriptions = SubscriptionTestFactory.mocked
      val subscription1F = testSubscriptions.addRunning()
      val subscription2F = testSubscriptions.addRunning()

      val subscription = createSubscription(testSubscriptions)
      subscription.start

      loggerFactory.assertLogs(
        for {
          subscription1 <- subscription1F
          // fail this subscription
          _ = subscription1.closeWithExn(RetryableExn)
          subscription2 <- subscription2F
          _ = subscription2.closeWithExn(FatalExn)
          // wait for the next subscription to occur
          closeReason <- subscription.closeReason.failed
        } yield { closeReason shouldBe FatalExn },
        _.warningMessage should include(
          "The sequencer subscription encountered an exception and will be restarted"
        ),
        _.errorMessage should include("Closing resilient sequencer subscription due to exception"),
        _.warningMessage should include("Underlying subscription failed to close"),
      )
    }

    "restart from last received counter" in {
      val testSubscriptions = SubscriptionTestFactory.mocked
      val subscription1F = testSubscriptions.addRunning()
      val subscription2F = testSubscriptions.addRunning()

      val subscription = createSubscription(testSubscriptions)
      subscription.start

      for {
        subscription1 <- subscription1F
        _ = subscription1.subscribedCounter shouldBe SequencerCounter(0)
        // indicate that we've processed the next event
        _ <- subscription1.handleCounter(43)
        // fail this subscription
        _ = subscription1.closeWithReason(RetryableError)
        // wait for the next subscription to occur
        subscription2 <- subscription2F
      } yield subscription2.subscribedCounter shouldBe SequencerCounter(43)
    }

    "correctly indicates whether we've received items when calculating the next retry delay" in {
      val hasReceivedEventsCalls = mutable.Buffer[Boolean]()
      val captureHasEvent = new SubscriptionRetryDelayRule {
        override def nextDelay(
            previousDelay: FiniteDuration,
            hasReceivedEvent: Boolean,
        ): FiniteDuration = {
          hasReceivedEventsCalls += hasReceivedEvent
          1.milli
        }
        override val initialDelay: FiniteDuration = 1.milli
        override val warnDelayDuration: FiniteDuration = 100.millis
      }
      val testSubscriptions = SubscriptionTestFactory.mocked
      val subscription1F = testSubscriptions.addRunning()
      val subscription2F = testSubscriptions.addRunning()
      val subscription3F = testSubscriptions.addRunning()

      val subscription = createSubscription(testSubscriptions, retryDelayRule = captureHasEvent)
      subscription.start

      for {
        subscription1 <- subscription1F
        _ <-
          // provide an event then close with a recoverable error
          subscription1
            .handleCounter(1)
            .map(_ => subscription1.closeWithReason(RetryableError))
        subscription2 <- subscription2F
        _ = {
          // don't provide an event and close immediately
          subscription2.closeWithReason(RetryableError)
        }
        subscription3 <- subscription3F
        _ = subscription3.closeWithReason(SubscriptionCloseReason.Closed)
        closeReason <- subscription.closeReason
      } yield {
        closeReason should be(SubscriptionCloseReason.Closed)
        hasReceivedEventsCalls.toList should contain theSameElementsInOrderAs List(true, false)
      }
    }

    "close underlying subscription even if created after we close" in {
      val resilientSequencerSubscriptionRef
          : AtomicReference[ResilientSequencerSubscription[TestHandlerError]] =
        new AtomicReference[ResilientSequencerSubscription[TestHandlerError]]()
      val closePromise = Promise[Unit]()

      val subscription = new SequencerSubscription[TestHandlerError] {
        override protected def timeouts = ResilientSequencerSubscriptionTest.this.timeouts
        override protected def loggerFactory: NamedLoggerFactory =
          ResilientSequencerSubscriptionTest.this.loggerFactory
        override private[canton] def complete(reason: SubscriptionCloseReason[TestHandlerError])(
            implicit traceContext: TraceContext
        ): Unit = ()
      }

      val subscriptionFactory =
        new SequencerSubscriptionFactory[TestHandlerError] {
          override def create(
              startingCounter: SequencerCounter,
              handler: SerializedEventHandler[TestHandlerError],
          )(implicit traceContext: TraceContext): UnlessShutdown[
            (SequencerSubscription[TestHandlerError], SubscriptionErrorRetryPolicy)
          ] = {
            // Close the resilient sequencer subscription while it is creating the subscription
            // close will block waiting for the subscription request, so start in a future but defer waiting for its completion until after its resolved
            closePromise.completeWith(Future { resilientSequencerSubscriptionRef.get().close() })
            eventually() {
              resilientSequencerSubscriptionRef.get().isClosing shouldBe true
            }
            Outcome(subscription -> TestSubscriptionError.retryRule)
          }
        }

      val resilientSequencerSubscription = new ResilientSequencerSubscription[TestHandlerError](
        domainId,
        SequencerCounter(0),
        _ => Future.successful[Either[TestHandlerError, Unit]](Right(())),
        subscriptionFactory,
        retryDelay(),
        timeouts,
        loggerFactory,
      )
      resilientSequencerSubscriptionRef.set(resilientSequencerSubscription)

      // kick off
      resilientSequencerSubscription.start

      for {
        _ <- closePromise.future
      } yield subscription.isClosing shouldBe true // should have called close on underlying subscription
    }

    "not create a subscription when the subscription factory returns AbortedDueToShutdown" in {

      val subscriptionFactory =
        new SequencerSubscriptionFactory[TestHandlerError] {
          override def create(
              startingCounter: SequencerCounter,
              handler: SerializedEventHandler[TestHandlerError],
          )(implicit traceContext: TraceContext): UnlessShutdown[
            (SequencerSubscription[TestHandlerError], SubscriptionErrorRetryPolicy)
          ] = AbortedDueToShutdown
        }

      val resilientSequencerSubscription = new ResilientSequencerSubscription[TestHandlerError](
        domainId,
        SequencerCounter(0),
        _ => Future.successful[Either[TestHandlerError, Unit]](Right(())),
        subscriptionFactory,
        retryDelay(),
        timeouts,
        loggerFactory,
      )
      // kick off
      resilientSequencerSubscription.start

      for {
        closeReason <- resilientSequencerSubscription.closeReason
      } yield {
        closeReason shouldBe SubscriptionCloseReason.Shutdown
        resilientSequencerSubscription.isClosing shouldBe true
      }
    }

  }
}

// these tests require a parallel execution context so are separated from the main group of tests
class ResilientSequencerSubscriptionRetryTimingTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ResilientSequencerSubscriptionTestUtils {

  "retry until closing if the sequencer is permanently unavailable" in {
    val startTime = Deadline.now
    val maxDelay = 100.milliseconds

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        val subscription =
          createSubscription(
            SubscriptionTestFactory.alwaysCloseWith(RetryableError),
            // this will retry with a max-delay and it will warn once the max-delay is hit
            retryDelayRule = retryDelay(maxDelay),
          )

        subscription.start

        // we retry until we see a warning
        eventually() {
          loggerFactory.numberOfRecordedEntries should be > 0
        }

        subscription.close()
      },
      logEntries => {
        forEvery(logEntries) {
          _.warningMessage should (include(s"Waiting $maxDelay before reconnecting") or include(
            LostSequencerSubscription.id
          ))
        }
        logEntries should not be empty
      },
    )
    // Check that it has hit MaxDelay. We can't really check an upper bound as it would make the test flaky
    -startTime.timeLeft should be >= maxDelay
  }
}

trait ResilientSequencerSubscriptionTestUtils {
  this: BaseTest with HasExecutionContext =>

  // very short to speedup test
  val InitialDelay: FiniteDuration = 1.millisecond
  val MaxDelay: FiniteDuration =
    1025.millis // 1 + power of 2 because InitialDelay keeps being doubled

  private lazy val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain1::test"))

  def retryDelay(maxDelay: FiniteDuration = MaxDelay) =
    SubscriptionRetryDelayRule(InitialDelay, maxDelay, maxDelay)

  def createSubscription(
      subscriptionTestFactory: SubscriptionTestFactory,
      retryDelayRule: SubscriptionRetryDelayRule = retryDelay(),
  ): ResilientSequencerSubscription[TestHandlerError] = {
    val subscription = new ResilientSequencerSubscription(
      domainId,
      SequencerCounter(0),
      _ => Future.successful[Either[TestHandlerError, Unit]](Right(())),
      subscriptionTestFactory,
      retryDelayRule,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )

    subscription
  }

  trait SubscriptionTestFactory extends SequencerSubscriptionFactory[TestHandlerError] {
    protected def createInternal(
        counter: SequencerCounter,
        handler: SerializedEventHandler[TestHandlerError],
    )(implicit traceContext: TraceContext): SequencerSubscription[TestHandlerError]

    override def create(
        startingCounter: SequencerCounter,
        handler: SerializedEventHandler[TestHandlerError],
    )(implicit
        traceContext: TraceContext
    ): UnlessShutdown[(SequencerSubscription[TestHandlerError], SubscriptionErrorRetryPolicy)] =
      Outcome(
        (createInternal(startingCounter, handler), TestSubscriptionError.retryRule)
      )
  }

  object SubscriptionTestFactory {
    def mocked: MockedSubscriptions = new MockedSubscriptions

    def alwaysCloseWith(
        reason: SubscriptionCloseReason[TestHandlerError]
    ): SubscriptionTestFactory =
      new SubscriptionTestFactory {
        override def createInternal(
            counter: SequencerCounter,
            handler: SerializedEventHandler[TestHandlerError],
        )(implicit traceContext: TraceContext): SequencerSubscription[TestHandlerError] =
          new SequencerSubscription[TestHandlerError] {
            override protected def loggerFactory: NamedLoggerFactory =
              ResilientSequencerSubscriptionTestUtils.this.loggerFactory
            closeReasonPromise.trySuccess(reason)
            override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
            override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Nil
            override private[canton] def complete(
                reason: SubscriptionCloseReason[TestHandlerError]
            )(implicit traceContext: TraceContext): Unit = ()
          }
      }
  }

  class MockSubscriptionResponse(
      mockCloseReason: Option[SubscriptionCloseReason[TestHandlerError]] = None
  ) {
    type SubscriberDetails =
      (SequencerCounter, SerializedEventHandler[TestHandlerError], MockedSequencerSubscription)
    private val activeSubscription =
      new AtomicReference[Option[SubscriberDetails]](None)
    private val subscribedP: Promise[Unit] = Promise()
    val subscribed = subscribedP.future

    class MockedSequencerSubscription(
        counter: SequencerCounter,
        handler: SerializedEventHandler[TestHandlerError],
    ) extends SequencerSubscription[TestHandlerError] {
      override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
      override protected def loggerFactory: NamedLoggerFactory =
        ResilientSequencerSubscriptionTestUtils.this.loggerFactory

      def closeWithReason(reason: SubscriptionCloseReason[TestHandlerError]): Boolean =
        closeReasonPromise.trySuccess(reason)
      def closeWithExn(exn: Throwable): Boolean = closeReasonPromise.tryFailure(exn)

      if (!activeSubscription.compareAndSet(None, Some((counter, handler, this)))) {
        fail("subscription has been created more than once")
      } else {
        subscribedP.trySuccess(())
      }

      override private[canton] def complete(reason: SubscriptionCloseReason[TestHandlerError])(
          implicit traceContext: TraceContext
      ): Unit = closeReasonPromise.trySuccess(reason).discard[Boolean]

      override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Nil

      // immediately close if a close reason was specified
      mockCloseReason foreach closeReasonPromise.trySuccess
    }

    def create(
        counter: SequencerCounter,
        handler: SerializedEventHandler[TestHandlerError],
    ): SequencerSubscription[TestHandlerError] =
      new MockedSequencerSubscription(counter, handler)

    private def fromSubscriber[A](getter: SubscriberDetails => A): A =
      activeSubscription.get() match {
        case Some(details) => getter(details)
        case None => fail("subscriber has not yet subscribed")
      }

    def handleCounter(sc: Long): Future[Either[TestHandlerError, Unit]] =
      fromSubscriber(_._2)(OrdinarySequencedEvent(deliverEvent(sc), None)(traceContext))

    def subscribedCounter: SequencerCounter = fromSubscriber(_._1)

    def closeWithReason(reason: SubscriptionCloseReason[TestHandlerError]): Boolean =
      fromSubscriber(_._3).closeWithReason(reason)

    def closeWithExn(exn: Throwable): Boolean =
      fromSubscriber(_._3).closeWithExn(exn)

    def subscription: MockedSequencerSubscription = fromSubscriber(_._3)

    private def deliverEvent(
        sc: Long
    ): SignedContent[SequencedEvent[ClosedEnvelope]] = {
      val deliver = SequencerTestUtils.mockDeliver(sc)
      SignedContent(deliver, SymbolicCrypto.emptySignature, None, testedProtocolVersion)
    }
  }

  class MockedSubscriptions extends SubscriptionTestFactory {
    private val subscriptions = scala.collection.mutable.Buffer[MockSubscriptionResponse]()
    private val nextSubscription = new AtomicInteger(0)

    def addRecoverable(): MockedSubscriptions = addClosed(RetryableError)

    def addUnrecoverable(): MockedSubscriptions = addClosed(UnretryableError)

    def addRunning(): Future[MockSubscriptionResponse] = {
      val mockResponse = new MockSubscriptionResponse()
      add(mockResponse)
      mockResponse.subscribed.map(_ => mockResponse)
    }

    def addClosed(reason: SubscriptionCloseReason[TestHandlerError]): MockedSubscriptions = {
      add(new MockSubscriptionResponse(Some(reason)))
    }

    def add(mockSubscriptionResponse: MockSubscriptionResponse): MockedSubscriptions = {
      subscriptions += mockSubscriptionResponse
      this
    }

    override def createInternal(
        counter: SequencerCounter,
        handler: SerializedEventHandler[TestHandlerError],
    )(implicit traceContext: TraceContext): SequencerSubscription[TestHandlerError] =
      subscriptions(nextSubscription.getAndIncrement()).create(counter, handler)

    def allShouldHaveBeenUsed: Assertion = nextSubscription.get() shouldBe subscriptions.length
  }

}
