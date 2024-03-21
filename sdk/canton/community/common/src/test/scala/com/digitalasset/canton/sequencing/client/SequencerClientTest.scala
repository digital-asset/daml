// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.metrics.api.MetricName
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{CryptoPureApi, Fingerprint, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NamedLoggingContext}
import com.digitalasset.canton.metrics.MetricHandle.NoOpMetricsFactory
import com.digitalasset.canton.metrics.{CommonMockMetrics, SequencerClientMetrics}
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.protocol.{DomainParametersLookup, TestDomainParameters}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.SequencedEventValidationError.GapInSequencerCounter
import com.digitalasset.canton.sequencing.client.SequencerClient.CloseReason.{
  ClientShutdown,
  UnrecoverableError,
}
import com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.{
  ApplicationHandlerException,
  EventValidationError,
}
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.{
  HandlerError,
  HandlerException,
}
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport
import com.digitalasset.canton.sequencing.handshake.HandshakeRequestError
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.memory.{
  InMemorySendTrackerStore,
  InMemorySequencedEventStore,
  InMemorySequencerCounterTrackerStore,
}
import com.digitalasset.canton.store.{
  CursorPrehead,
  SequencedEventStore,
  SequencerCounterTrackerStore,
}
import com.digitalasset.canton.time.{DomainTimeTracker, MockTimeRequestSubmitter, SimClock}
import com.digitalasset.canton.topology.DefaultTestIdentities.{participant1, sequencerId}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{DomainTopologyClient, TopologySnapshot}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

class SequencerClientTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutorService
    with CloseableTest {

  implicit lazy val executionContext: ExecutionContext = executorService

  private lazy val metrics =
    new SequencerClientMetrics(
      MetricName("SequencerClientTest"),
      NoOpMetricsFactory,
    )
  private lazy val firstSequencerCounter = SequencerCounter(42L)
  private lazy val deliver: Deliver[Nothing] =
    SequencerTestUtils.mockDeliver(
      firstSequencerCounter.unwrap,
      CantonTimestamp.Epoch,
      DefaultTestIdentities.domainId,
    )
  private lazy val signedDeliver: OrdinarySerializedEvent = {
    OrdinarySequencedEvent(SequencerTestUtils.sign(deliver), None)(traceContext)
  }

  private lazy val nextDeliver: Deliver[Nothing] = SequencerTestUtils.mockDeliver(
    43,
    CantonTimestamp.ofEpochSecond(1),
    DefaultTestIdentities.domainId,
  )
  private lazy val deliver44: Deliver[Nothing] = SequencerTestUtils.mockDeliver(
    44,
    CantonTimestamp.ofEpochSecond(2),
    DefaultTestIdentities.domainId,
  )
  private lazy val deliver45: Deliver[Nothing] = SequencerTestUtils.mockDeliver(
    45,
    CantonTimestamp.ofEpochSecond(3),
    DefaultTestIdentities.domainId,
  )

  def deliver(i: Long): Deliver[Nothing] = SequencerTestUtils.mockDeliver(
    i,
    CantonTimestamp.Epoch.plusSeconds(i),
    DefaultTestIdentities.domainId,
  )

  private lazy val alwaysSuccessfulHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
    ApplicationHandler.success()
  private lazy val failureException = new IllegalArgumentException("application handler failed")
  private lazy val alwaysFailingHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
    ApplicationHandler.create("always-fails")(_ =>
      HandlerResult.synchronous(FutureUnlessShutdown.failed(failureException))
    )

  "subscribe" should {
    "throws if more than one handler is subscribed" in {
      (for {
        env <- Env.create()
        _ <- env.subscribeAfter()
        error <- loggerFactory
          .assertLogs(
            env.subscribeAfter(CantonTimestamp.MinValue, alwaysSuccessfulHandler),
            _.warningMessage shouldBe "Cannot create additional subscriptions to the sequencer from the same client",
          )
          .failed
      } yield error).futureValue shouldBe a[RuntimeException]
    }

    "start from the specified sequencer counter if there is no recorded event" in {
      val counterF = for {
        env <- Env.create(initialSequencerCounter = SequencerCounter(5))
        _ <- env.subscribeAfter()
      } yield env.transport.subscriber.value.request.counter

      counterF.futureValue shouldBe SequencerCounter(5)
    }

    "starts subscription at last stored event (for fork verification)" in {
      val counterF = for {
        env <- Env.create(
          storedEvents = Seq(deliver)
        )
        _ <- env.subscribeAfter()
      } yield env.transport.subscriber.value.request.counter

      counterF.futureValue shouldBe deliver.counter
    }

    "stores the event in the SequencedEventStore" in {
      val storedEventF = for {
        env @ Env(client, transport, _, sequencedEventStore, _) <- Env.create()

        _ <- env.subscribeAfter()
        _ <- transport.subscriber.value.handler(signedDeliver)
        _ <- client.flush()
        storedEvent <- sequencedEventStore.sequencedEvents()
      } yield storedEvent

      storedEventF.futureValue shouldBe Seq(signedDeliver)
    }

    "stores the event even if the handler fails" in {
      val storedEventF = for {
        env @ Env(client, transport, _, sequencedEventStore, _) <- Env.create()

        _ <- env.subscribeAfter(eventHandler = alwaysFailingHandler)
        _ <- loggerFactory.assertLogs(
          {
            for {
              _ <- transport.subscriber.value.handler(signedDeliver)
              _ <- client.flush()
            } yield ()
          },
          logEntry => {
            logEntry.errorMessage should be(
              "Synchronous event processing failed for event batch with sequencer counters 42 to 42."
            )
            logEntry.throwable.value shouldBe failureException
          },
        )
        storedEvent <- sequencedEventStore.sequencedEvents()
      } yield storedEvent

      storedEventF.futureValue shouldBe Seq(signedDeliver)
    }

    "doesn't give prior event to the application handler" in {
      val validated = new AtomicBoolean()
      val processed = new AtomicBoolean()
      val testF = for {
        env @ Env(_client, transport, _, _, _) <- Env.create(
          eventValidator = new SequencedEventValidator {
            override def validate(
                priorEvent: Option[PossiblyIgnoredSerializedEvent],
                event: OrdinarySerializedEvent,
                sequencerId: SequencerId,
            ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit] = {
              validated.set(true)
              Env.eventAlwaysValid.validate(priorEvent, event, sequencerId)
            }

            override def validateOnReconnect(
                priorEvent: Option[PossiblyIgnoredSerializedEvent],
                reconnectEvent: OrdinarySerializedEvent,
                sequencerId: SequencerId,
            ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit] =
              validate(priorEvent, reconnectEvent, sequencerId)

            override def validatePekko[E: Pretty](
                subscription: SequencerSubscriptionPekko[E],
                priorReconnectEvent: Option[OrdinarySerializedEvent],
                sequencerId: SequencerId,
            )(implicit
                traceContext: TraceContext
            ): SequencerSubscriptionPekko[SequencedEventValidationError[E]] = ???

            override def close(): Unit = ()
          },
          storedEvents = Seq(deliver),
        )
        _ <- env.subscribeAfter(
          deliver.timestamp,
          ApplicationHandler.create("") { events =>
            processed.set(true)
            alwaysSuccessfulHandler(events)
          },
        )
        _ = transport.subscriber.value.request.counter shouldBe deliver.counter
        _ <- transport.subscriber.value.handler(signedDeliver)
      } yield {
        validated.get() shouldBe true
        processed.get() shouldBe false
      }

      testF.futureValue
    }

    "picks the last prior event" in {
      val triggerNextDeliverHandling = new AtomicBoolean()
      val testF = for {
        env <- Env.create(
          storedEvents = Seq(deliver, nextDeliver, deliver44)
        )
        _ <- env.subscribeAfter(
          nextDeliver.timestamp.immediatePredecessor,
          ApplicationHandler.create("") { events =>
            if (events.value.exists(_.counter == nextDeliver.counter)) {
              triggerNextDeliverHandling.set(true)
            }
            HandlerResult.done
          },
        )
      } yield ()

      testF.futureValue
      triggerNextDeliverHandling.get shouldBe true
    }

    "completes the sequencer client if the subscription closes due to an error" in {
      val error =
        EventValidationError(GapInSequencerCounter(SequencerCounter(666), SequencerCounter(0)))
      val closeReasonF = for {
        env @ Env(client, transport, _, _, _) <- Env.create(useParallelExecutionContext = true)

        _ <- env.subscribeAfter(CantonTimestamp.MinValue, alwaysSuccessfulHandler)
        subscription = transport.subscriber
          // we know the resilient sequencer subscription is using this type
          .map(_.subscription.asInstanceOf[MockSubscription[SequencerClientSubscriptionError]])
          .value
        closeReason <- loggerFactory.assertLogs(
          {
            subscription.closeSubscription(error)
            client.completion
          },
          _.warningMessage should include("sequencer"),
        )
      } yield closeReason

      closeReasonF.futureValue should matchPattern {
        case e: UnrecoverableError if e.cause == s"handler returned error: $error" =>
      }
    }

    "completes the sequencer client if the application handler fails" in {
      val error = new RuntimeException("failed handler")
      val syncError = ApplicationHandlerException(error, deliver.counter, deliver.counter)
      val handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
        ApplicationHandler.create("async-failure")(_ =>
          FutureUnlessShutdown.failed[AsyncResult](error)
        )

      val closeReasonF = for {
        env @ Env(client, transport, _, _, _) <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter(CantonTimestamp.MinValue, handler)
        closeReason <- loggerFactory.assertLogs(
          {
            for {
              _ <- transport.subscriber.value.sendToHandler(deliver)
              // Send the next event so that the client notices that an error has occurred.
              _ <- client.flush()
              _ <- transport.subscriber.value.sendToHandler(nextDeliver)
              // wait until the subscription is closed (will emit an error)
              closeReason <- client.completion
            } yield closeReason
          },
          logEntry => {
            logEntry.errorMessage should be(
              s"Synchronous event processing failed for event batch with sequencer counters ${deliver.counter} to ${deliver.counter}."
            )
            logEntry.throwable shouldBe Some(error)
          },
          _.warningMessage should include(
            s"Closing resilient sequencer subscription due to error: HandlerError($syncError)"
          ),
        )

      } yield {
        client.close() // make sure that we can still close the sequencer client
        closeReason
      }

      closeReasonF.futureValue should matchPattern {
        case e: UnrecoverableError if e.cause == s"handler returned error: $syncError" =>
      }
    }

    "completes the sequencer client if the application handler shuts down synchronously" in {
      val handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
        ApplicationHandler.create("shutdown")(_ => FutureUnlessShutdown.abortedDueToShutdown)

      val closeReasonF = for {
        env @ Env(client, transport, _, _, _) <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter(eventHandler = handler)
        closeReason <- {
          for {
            _ <- transport.subscriber.value.sendToHandler(deliver)
            // Send the next event so that the client notices that an error has occurred.
            _ <- client.flush()
            _ <- transport.subscriber.value.sendToHandler(nextDeliver)
            closeReason <- client.completion
          } yield closeReason
        }
      } yield {
        client.close() // make sure that we can still close the sequencer client
        closeReason
      }

      closeReasonF.futureValue shouldBe ClientShutdown
    }

    "completes the sequencer client if asynchronous event processing fails" in {
      val error = new RuntimeException("asynchronous failure")
      val asyncFailure = HandlerResult.asynchronous(FutureUnlessShutdown.failed(error))
      val asyncException = ApplicationHandlerException(error, deliver.counter, deliver.counter)

      val closeReasonF = for {
        env @ Env(client, transport, _, _, _) <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter(
          eventHandler = ApplicationHandler.create("async-failure")(_ => asyncFailure)
        )
        closeReason <- loggerFactory.assertLogs(
          {
            for {
              _ <- transport.subscriber.value.sendToHandler(deliver)
              // Make sure that the asynchronous error has been noticed
              // We intentionally do two flushes. The first captures `handleReceivedEventsUntilEmpty` completing.
              // During this it may addToFlush a future for capturing `asyncSignalledF` however this may occur
              // after we've called `flush` and therefore won't guarantee completing all processing.
              // So our second flush will capture `asyncSignalledF` for sure.
              _ <- client.flush()
              _ <- client.flush()
              // Send the next event so that the client notices that an error has occurred.
              _ <- transport.subscriber.value.sendToHandler(nextDeliver)
              _ <- client.flush()
              // wait until client completed (will write an error)
              closeReason <- client.completion
              _ = client.close() // make sure that we can still close the sequencer client
            } yield closeReason
          },
          logEntry => {
            logEntry.errorMessage should include(
              s"Asynchronous event processing failed for event batch with sequencer counters ${deliver.counter} to ${deliver.counter}"
            )
            logEntry.throwable shouldBe Some(error)
          },
          _.warningMessage should include(
            s"Closing resilient sequencer subscription due to error: HandlerError($asyncException)"
          ),
        )
      } yield closeReason

      closeReasonF.futureValue should matchPattern {
        case e: UnrecoverableError if e.cause == s"handler returned error: $asyncException" =>
      }
    }

    "completes the sequencer client if asynchronous event processing shuts down" in {
      val asyncShutdown = HandlerResult.asynchronous(FutureUnlessShutdown.abortedDueToShutdown)

      val closeReasonF = for {
        env @ Env(client, transport, _, _, _) <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter(
          CantonTimestamp.MinValue,
          ApplicationHandler.create("async-shutdown")(_ => asyncShutdown),
        )
        closeReason <- {
          for {
            _ <- transport.subscriber.value.sendToHandler(deliver)
            _ <- client.flushClean() // Make sure that the asynchronous error has been noticed
            // Send the next event so that the client notices that an error has occurred.
            _ <- transport.subscriber.value.sendToHandler(nextDeliver)
            _ <- client.flush()
            closeReason <- client.completion
          } yield closeReason
        }
      } yield {
        client.close() // make sure that we can still close the sequencer client
        closeReason
      }

      closeReasonF.futureValue shouldBe ClientShutdown
    }

    "replays messages from the SequencedEventStore" in {
      val processedEvents = new ConcurrentLinkedQueue[SequencerCounter]

      val testF = for {
        env <- Env.create(
          storedEvents = Seq(deliver, nextDeliver, deliver44)
        )
        _ <- env.subscribeAfter(
          deliver.timestamp,
          ApplicationHandler.create("") { events =>
            events.value.foreach(event => processedEvents.add(event.counter))
            alwaysSuccessfulHandler(events)
          },
        )
      } yield ()

      testF.futureValue

      processedEvents.iterator().asScala.toSeq shouldBe Seq(
        nextDeliver.counter,
        deliver44.counter,
      )
    }

    "propagates errors during replay" in {
      val syncError =
        ApplicationHandlerException(failureException, nextDeliver.counter, deliver44.counter)
      val syncExc = SequencerClientSubscriptionException(syncError)

      val errorF = for {
        env <- Env.create(
          storedEvents = Seq(deliver, nextDeliver, deliver44)
        )
        error <- loggerFactory.assertLogs(
          env.subscribeAfter(deliver.timestamp, alwaysFailingHandler).failed,
          logEntry => {
            logEntry.errorMessage shouldBe "Synchronous event processing failed for event batch with sequencer counters 43 to 44."
            logEntry.throwable shouldBe Some(failureException)
          },
          logEntry => {
            logEntry.errorMessage should include("Sequencer subscription failed")
            logEntry.throwable shouldBe Some(syncExc)
          },
        )
      } yield error

      errorF.futureValue shouldBe syncExc
    }

    "throttle message batches" in {
      val counter = new AtomicInteger(0)
      val maxSeenCounter = new AtomicInteger(0)
      for {
        env <- Env.create(
          options = SequencerClientConfig(
            eventInboxSize = PositiveInt.tryCreate(1),
            maximumInFlightEventBatches = PositiveInt.tryCreate(5),
          ),
          useParallelExecutionContext = true,
          initialSequencerCounter = SequencerCounter(1L),
        )
        _ <- env.subscribeAfter(
          CantonTimestamp.Epoch,
          ApplicationHandler.create("test-handler-throttling") { e =>
            HandlerResult.asynchronous(
              FutureUnlessShutdown.outcomeF(Future {
                blocking {
                  maxSeenCounter.synchronized {
                    maxSeenCounter.set(Math.max(counter.incrementAndGet(), maxSeenCounter.get()))
                  }
                }
                Threading.sleep(100)
                counter.decrementAndGet().discard
              }(SequencerClientTest.this.executorService))
            )
          },
        )

        _ <- MonadUtil.sequentialTraverse_(1 to 100) { i =>
          env.transport.subscriber.value.sendToHandler(deliver(i.toLong))
        }
      } yield {
        maxSeenCounter.get() shouldBe 5
      }
    }
  }

  "subscribeTracking" should {
    "updates sequencer counter prehead" in {
      val preHeadF = for {
        Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) <- Env.create()

        _ <- client.subscribeTracking(
          sequencerCounterTrackerStore,
          alwaysSuccessfulHandler,
          timeTracker,
        )
        _ <- transport.subscriber.value.handler(signedDeliver)
        _ <- client.flushClean()
        preHead <- sequencerCounterTrackerStore.preheadSequencerCounter
      } yield preHead.value

      preHeadF.futureValue shouldBe CursorPrehead(deliver.counter, deliver.timestamp)
    }

    "replays from the sequencer counter prehead" in {
      val processedEvents = new ConcurrentLinkedQueue[SequencerCounter]

      val preheadF = for {
        Env(client, _transport, sequencerCounterTrackerStore, _, timeTracker) <- Env.create(
          storedEvents = Seq(deliver, nextDeliver, deliver44, deliver45),
          cleanPrehead = Some(CursorPrehead(nextDeliver.counter, nextDeliver.timestamp)),
        )

        _ <- client.subscribeTracking(
          sequencerCounterTrackerStore,
          ApplicationHandler.create("") { events =>
            events.value.foreach(event => processedEvents.add(event.counter))
            alwaysSuccessfulHandler(events)
          },
          timeTracker,
        )
        _ <- client.flushClean()
        prehead <- sequencerCounterTrackerStore.preheadSequencerCounter
      } yield prehead.value

      preheadF.futureValue shouldBe CursorPrehead(deliver45.counter, deliver45.timestamp)
      processedEvents.iterator().asScala.toSeq shouldBe Seq(
        deliver44.counter,
        deliver45.counter,
      )

    }

    "resubscribes after replay" in {
      val processedEvents = new ConcurrentLinkedQueue[SequencerCounter]

      val preheadF = for {
        Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) <- Env.create(
          storedEvents = Seq(deliver, nextDeliver, deliver44),
          cleanPrehead = Some(CursorPrehead(nextDeliver.counter, nextDeliver.timestamp)),
        )
        _ <- client.subscribeTracking(
          sequencerCounterTrackerStore,
          ApplicationHandler.create("") { events =>
            events.value.foreach(event => processedEvents.add(event.counter))
            alwaysSuccessfulHandler(events)
          },
          timeTracker,
        )
        _ <- transport.subscriber.value.sendToHandler(deliver45)
        _ <- client.flushClean()
        prehead <- sequencerCounterTrackerStore.preheadSequencerCounter
      } yield prehead.value

      preheadF.futureValue shouldBe CursorPrehead(deliver45.counter, deliver45.timestamp)

      processedEvents.iterator().asScala.toSeq shouldBe Seq(
        deliver44.counter,
        deliver45.counter,
      )
    }

    "does not update the prehead if the application handler fails" in {
      val preHeadF = for {
        Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) <- Env.create()

        _ <- client.subscribeTracking(
          sequencerCounterTrackerStore,
          alwaysFailingHandler,
          timeTracker,
        )
        _ <- loggerFactory.assertLogs(
          {
            for {
              _ <- transport.subscriber.value.handler(signedDeliver)
              _ <- client.flushClean()
            } yield ()
          },
          logEntry => {
            logEntry.errorMessage should be(
              "Synchronous event processing failed for event batch with sequencer counters 42 to 42."
            )
            logEntry.throwable.value shouldBe failureException
          },
        )
        preHead <- sequencerCounterTrackerStore.preheadSequencerCounter
      } yield preHead

      preHeadF.futureValue shouldBe None
    }

    "updates the prehead only after the asynchronous processing has been completed" in {
      val promises = Map[SequencerCounter, Promise[UnlessShutdown[Unit]]](
        nextDeliver.counter -> Promise[UnlessShutdown[Unit]](),
        deliver44.counter -> Promise[UnlessShutdown[Unit]](),
      )

      def handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
        ApplicationHandler.create("") { events =>
          assert(events.value.size == 1)
          promises.get(events.value(0).counter) match {
            case None => HandlerResult.done
            case Some(promise) => HandlerResult.asynchronous(FutureUnlessShutdown(promise.future))
          }
        }

      val testF = for {
        Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) <- Env.create(
          options = SequencerClientConfig(eventInboxSize = PositiveInt.tryCreate(1))
        )
        _ <- client.subscribeTracking(sequencerCounterTrackerStore, handler, timeTracker)
        _ <- transport.subscriber.value.sendToHandler(deliver)
        _ <- client.flushClean()
        prehead42 <- sequencerCounterTrackerStore.preheadSequencerCounter
        _ <- transport.subscriber.value.sendToHandler(nextDeliver)
        prehead43 <- sequencerCounterTrackerStore.preheadSequencerCounter
        _ <- transport.subscriber.value.sendToHandler(deliver44)
        _ = promises(deliver44.counter).success(UnlessShutdown.unit)
        prehead43a <- sequencerCounterTrackerStore.preheadSequencerCounter
        _ = promises(nextDeliver.counter).success(
          UnlessShutdown.unit
        ) // now we can advance the prehead
        _ <- client.flushClean()
        prehead44 <- sequencerCounterTrackerStore.preheadSequencerCounter
      } yield {
        prehead42 shouldBe Some(CursorPrehead(deliver.counter, deliver.timestamp))
        prehead43 shouldBe Some(CursorPrehead(deliver.counter, deliver.timestamp))
        prehead43a shouldBe Some(CursorPrehead(deliver.counter, deliver.timestamp))
        prehead44 shouldBe Some(CursorPrehead(deliver44.counter, deliver44.timestamp))
      }

      testF.futureValue
    }
  }

  "changeTransport" should {
    "create second subscription from the same counter as the previous one when there are no events" in {
      val secondTransport = new MockTransport
      val testF = for {
        env <- Env.create(
          useParallelExecutionContext = true,
          initialSequencerCounter = SequencerCounter.Genesis,
        )
        _ <- env.subscribeAfter()
        _ <- env.changeTransport(secondTransport)
      } yield {
        val originalSubscriber = env.transport.subscriber.value
        originalSubscriber.request.counter shouldBe SequencerCounter.Genesis
        originalSubscriber.subscription.isClosing shouldBe true // old subscription gets closed
        env.transport.isClosing shouldBe true

        val newSubscriber = secondTransport.subscriber.value
        newSubscriber.request.counter shouldBe SequencerCounter.Genesis
        newSubscriber.subscription.isClosing shouldBe false
        secondTransport.isClosing shouldBe false

        env.client.completion.isCompleted shouldBe false
      }

      testF.futureValue
    }

    "create second subscription from the same counter as the previous one when there are events" in {
      val secondTransport = new MockTransport

      val testF = for {
        env <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter()

        _ <- env.transport.subscriber.value.sendToHandler(deliver)
        _ <- env.transport.subscriber.value.sendToHandler(nextDeliver)
        _ <- env.client.flushClean()

        _ <- env.changeTransport(secondTransport)
      } yield {
        val originalSubscriber = env.transport.subscriber.value
        originalSubscriber.request.counter shouldBe firstSequencerCounter

        val newSubscriber = secondTransport.subscriber.value
        newSubscriber.request.counter shouldBe nextDeliver.counter

        env.client.completion.isCompleted shouldBe false
      }

      testF.futureValue
    }

    "have new transport be used for sends" in {
      val secondTransport = new MockTransport

      val testF = for {
        env <- Env.create(useParallelExecutionContext = true)
        _ <- env.changeTransport(secondTransport)
        _ <- env.sendAsync(Batch.empty(testedProtocolVersion))
      } yield {
        env.transport.lastSend.get() shouldBe None
        secondTransport.lastSend.get() should not be None

        env.transport.isClosing shouldBe true
        secondTransport.isClosing shouldBe false
      }

      testF.futureValue
    }

    "have new transport be used for sends when there is subscription" in {
      val secondTransport = new MockTransport

      val testF = for {
        env <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter()
        _ <- env.changeTransport(secondTransport)
        _ <- env.sendAsync(Batch.empty(testedProtocolVersion))
      } yield {
        env.transport.lastSend.get() shouldBe None
        secondTransport.lastSend.get() should not be None
      }

      testF.futureValue
    }

    "have new transport be used with same sequencerId but different sequencer alias" in {
      val secondTransport = new MockTransport

      val testF = for {
        env <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter()
        _ <- env.changeTransport(
          SequencerTransports.single(
            SequencerAlias.tryCreate("somethingElse"),
            sequencerId,
            secondTransport,
          )
        )
        _ <- env.sendAsync(Batch.empty(testedProtocolVersion))
      } yield {
        env.transport.lastSend.get() shouldBe None
        secondTransport.lastSend.get() should not be None
      }

      testF.futureValue
    }

    "fail to reassign sequencerId" in {
      val secondTransport = new MockTransport
      val secondSequencerId = SequencerId(
        UniqueIdentifier(Identifier.tryCreate("da2"), Namespace(Fingerprint.tryCreate("default")))
      )

      val testF = for {
        env <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter()
        error <- loggerFactory
          .assertLogs(
            env
              .changeTransport(
                SequencerTransports.default(
                  secondSequencerId,
                  secondTransport,
                )
              ),
            _.errorMessage shouldBe "Adding or removing sequencer subscriptions is not supported at the moment",
          )
          .failed
      } yield {
        error
      }

      testF.futureValue shouldBe an[IllegalArgumentException]
      testF.futureValue.getMessage shouldBe "Adding or removing sequencer subscriptions is not supported at the moment"
    }
  }

  private case class Subscriber[E](
      request: SubscriptionRequest,
      handler: SerializedEventHandler[E],
      subscription: MockSubscription[E],
  ) {
    def sendToHandler(event: SequencedEvent[ClosedEnvelope]): Future[Unit] = {
      handler(OrdinarySequencedEvent(SequencerTestUtils.sign(event), None)(traceContext))
        .transform {
          case Success(Right(_)) => Success(())
          case Success(Left(err)) =>
            subscription.closeSubscription(err)
            Success(())
          case Failure(ex) =>
            subscription.closeSubscription(ex)
            Success(())
        }
    }
  }

  private case class Env(
      client: SequencerClientImpl,
      transport: MockTransport,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      sequencedEventStore: SequencedEventStore,
      timeTracker: DomainTimeTracker,
  ) {

    def subscribeAfter(
        priorTimestamp: CantonTimestamp = CantonTimestamp.MinValue,
        eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] = alwaysSuccessfulHandler,
    ): Future[Unit] =
      client.subscribeAfter(
        priorTimestamp,
        None,
        eventHandler,
        timeTracker,
        PeriodicAcknowledgements.noAcknowledgements,
      )

    def changeTransport(newTransport: SequencerClientTransport): Future[Unit] = {
      client.changeTransport(
        SequencerTransports.default(sequencerId, newTransport)
      )
    }

    def changeTransport(sequencerTransports: SequencerTransports): Future[Unit] =
      client.changeTransport(sequencerTransports)

    def sendAsync(
        batch: Batch[DefaultOpenEnvelope]
    )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
      client.sendAsync(batch)

  }

  private class MockSubscription[E] extends SequencerSubscription[E] {
    override protected def loggerFactory: NamedLoggerFactory =
      SequencerClientTest.this.loggerFactory

    override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing

    override private[canton] def complete(reason: SubscriptionCloseReason[E])(implicit
        traceContext: TraceContext
    ): Unit = {
      closeReasonPromise.success(reason)
      close()
    }

    def closeSubscription(reason: E): Unit = this.closeReasonPromise.success(HandlerError(reason))

    def closeSubscription(error: Throwable): Unit =
      this.closeReasonPromise.success(HandlerException(error))
  }

  private class MockTransport extends SequencerClientTransport with NamedLogging {

    override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing

    private val subscriberRef = new AtomicReference[Option[Subscriber[_]]](None)

    // When using a parallel execution context, the order of asynchronous operations within the SequencerClient
    // is not deterministic which can delay the subscription. This is why we add some retry policy to avoid flaky tests
    def subscriber: Option[Subscriber[_]] = {
      @tailrec def subscriber(retry: Int): Option[Subscriber[_]] =
        subscriberRef.get() match {
          case Some(value) => Some(value)
          case None if retry >= 0 =>
            logger.debug(
              s"Subscriber reference is not defined, will retry after sleeping. Retry: $retry"
            )
            Threading.sleep(5)
            subscriber(retry - 1)
          case None => None
        }

      subscriber(retry = 10)
    }

    val lastSend = new AtomicReference[Option[SubmissionRequest]](None)

    override def acknowledge(request: AcknowledgeRequest)(implicit
        traceContext: TraceContext
    ): Future[Unit] =
      Future.unit

    override def acknowledgeSigned(request: SignedContent[AcknowledgeRequest])(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, Unit] =
      EitherT.rightT(())

    override def sendAsync(
        request: SubmissionRequest,
        timeout: Duration,
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncClientError, Unit] = {
      lastSend.set(Some(request))
      EitherT.rightT(())
    }

    override def sendAsyncSigned(
        request: SignedContent[SubmissionRequest],
        timeout: Duration,
    )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
      sendAsync(request.content, timeout)

    override def sendAsyncUnauthenticated(
        request: SubmissionRequest,
        timeout: Duration,
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncClientError, Unit] = ???

    override def subscribe[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(
        implicit traceContext: TraceContext
    ): SequencerSubscription[E] = {
      val subscription = new MockSubscription[E]

      if (!subscriberRef.compareAndSet(None, Some(Subscriber(request, handler, subscription)))) {
        fail("subscribe has already been called by this client")
      }

      subscription
    }

    override def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
      SubscriptionErrorRetryPolicy.never

    override def handshake(request: HandshakeRequest)(implicit
        traceContext: TraceContext
    ): EitherT[Future, HandshakeRequestError, HandshakeResponse] = ???

    override protected def loggerFactory: NamedLoggerFactory =
      SequencerClientTest.this.loggerFactory

    override def subscribeUnauthenticated[E](
        request: SubscriptionRequest,
        handler: SerializedEventHandler[E],
    )(implicit traceContext: TraceContext): SequencerSubscription[E] = ???
  }

  private implicit class RichSequencerClient(client: SequencerClientImpl) {
    // flush needs to be called twice in order to finish asynchronous processing
    // (see comment around shutdown in SequencerClient). So we have this small
    // helper for the tests.
    def flushClean(): Future[Unit] = for {
      _ <- client.flush()
      _ <- client.flush()
    } yield ()
  }

  private object Env {
    val eventAlwaysValid: SequencedEventValidator = SequencedEventValidator.noValidation(
      DefaultTestIdentities.domainId,
      warn = false,
    )

    /** @param useParallelExecutionContext Set to true to use a parallel execution context which is handy for
      *                                     verifying close behavior that can involve an Await that would deadlock
      *                                     on ScalaTest's default serial execution context. However by enabling this
      *                                     it means the order of asynchronous operations within the SequencerClient
      *                                     will no longer be deterministic.
      */
    def create(
        storedEvents: Seq[SequencedEvent[ClosedEnvelope]] = Seq.empty,
        cleanPrehead: Option[SequencerCounterCursorPrehead] = None,
        eventValidator: SequencedEventValidator = eventAlwaysValid,
        options: SequencerClientConfig = SequencerClientConfig(),
        useParallelExecutionContext: Boolean = false,
        initialSequencerCounter: SequencerCounter = firstSequencerCounter,
    )(implicit closeContext: CloseContext): Future[Env] = {
      // if parallel execution is desired use the UseExecutorService executor service (which is a parallel execution context)
      // otherwise use the default serial execution context provided by ScalaTest
      implicit val executionContext: ExecutionContext =
        if (useParallelExecutionContext) SequencerClientTest.this.executorService
        else SequencerClientTest.this.executionContext
      val clock = new SimClock(loggerFactory = loggerFactory)
      val timeouts = DefaultProcessingTimeouts.testing
      val transport = new MockTransport
      val sendTrackerStore = new InMemorySendTrackerStore()
      val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory)
      val sendTracker =
        new SendTracker(Map.empty, sendTrackerStore, metrics, loggerFactory, timeouts)
      val sequencerCounterTrackerStore =
        new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
      val timeTracker =
        new DomainTimeTracker(
          DomainTimeTrackerConfig(),
          clock,
          new MockTimeRequestSubmitter(),
          timeouts,
          loggerFactory,
        )
      val domainParameters = BaseTest.defaultStaticDomainParameters

      val eventValidatorFactory = new SequencedEventValidatorFactory {
        override def create(
            unauthenticated: Boolean
        )(implicit loggingContext: NamedLoggingContext): SequencedEventValidator =
          eventValidator
      }
      val topologyClient = mock[DomainTopologyClient]
      val mockTopologySnapshot = mock[TopologySnapshot]
      when(topologyClient.currentSnapshotApproximation(any[TraceContext]))
        .thenReturn(mockTopologySnapshot)
      when(
        mockTopologySnapshot.findDynamicDomainParametersOrDefault(
          any[ProtocolVersion],
          anyBoolean,
        )(any[TraceContext])
      )
        .thenReturn(
          Future.successful(TestDomainParameters.defaultDynamic)
        )
      val maxRequestSizeLookup =
        DomainParametersLookup.forSequencerDomainParameters(
          domainParameters,
          None,
          topologyClient,
          FutureSupervisor.Noop,
          loggerFactory,
        )

      val client = new SequencerClientImpl(
        DefaultTestIdentities.domainId,
        participant1,
        SequencerTransports.default(DefaultTestIdentities.sequencerId, transport),
        options,
        TestingConfigInternal(),
        domainParameters.protocolVersion,
        maxRequestSizeLookup,
        timeouts,
        eventValidatorFactory,
        clock,
        new RequestSigner {
          override def signRequest[A <: HasCryptographicEvidence](
              request: A,
              hashPurpose: HashPurpose,
          )(implicit
              ec: ExecutionContext,
              traceContext: TraceContext,
          ): EitherT[Future, String, SignedContent[A]] =
            EitherT(
              Future.successful(
                Either.right[String, SignedContent[A]](
                  SignedContent(request, SymbolicCrypto.emptySignature, None, testedProtocolVersion)
                )
              )
            )
        },
        sequencedEventStore,
        sendTracker,
        CommonMockMetrics.sequencerClient,
        None,
        false,
        mock[CryptoPureApi],
        LoggingConfig(),
        loggerFactory,
        futureSupervisor,
        initialSequencerCounter,
      )(executionContext, tracer)
      val signedEvents = storedEvents.map(SequencerTestUtils.sign)

      for {
        _ <- sequencedEventStore.store(
          signedEvents.map(OrdinarySequencedEvent(_, None)(TraceContext.empty))
        )
        _ <- cleanPrehead.traverse_(prehead =>
          sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(prehead)
        )
      } yield Env(client, transport, sequencerCounterTrackerStore, sequencedEventStore, timeTracker)
    }
  }
}
