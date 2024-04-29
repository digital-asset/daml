// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{MetricName, MetricsContext}
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{CryptoPureApi, Fingerprint, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.HealthComponent.AlwaysHealthyComponent
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NamedLoggingContext}
import com.digitalasset.canton.metrics.{CommonMockMetrics, SequencerClientMetrics}
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.protocol.{
  DomainParametersLookup,
  DynamicDomainParametersLookup,
  TestDomainParameters,
}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.SendAsyncClientResponseError
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
import com.digitalasset.canton.sequencing.client.transports.{
  SequencerClientTransport,
  SequencerClientTransportPekko,
}
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
import com.digitalasset.canton.topology.DefaultTestIdentities.{daSequencerId, participant1}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{DomainTopologyClient, TopologySnapshot}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

class SequencerClientTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with CloseableTest
    with BeforeAndAfterAll {

  private lazy val metrics =
    new SequencerClientMetrics(
      MetricName("SequencerClientTest"),
      NoOpMetricsFactory,
    )(MetricsContext.Empty)
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

  private var actorSystem: ActorSystem = _
  private lazy val materializer: Materializer = Materializer(actorSystem)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    actorSystem = ActorSystem("SequencerClientTest")
  }

  override def afterAll(): Unit = {
    actorSystem.terminate().futureValue
    super.afterAll()
  }

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

  private def sequencerClient(factory: EnvFactory[SequencerClient]): Unit = {
    "subscribe" should {
      "throws if more than one handler is subscribed" in {
        val env = factory.create()
        env.subscribeAfter().futureValue
        loggerFactory.assertLogs(
          env.subscribeAfter(CantonTimestamp.MinValue, alwaysSuccessfulHandler).failed.futureValue,
          _.warningMessage shouldBe "Cannot create additional subscriptions to the sequencer from the same client",
          _.errorMessage should include("Sequencer subscription failed"),
        ) shouldBe a[RuntimeException]
        env.client.close()
      }

      "start from the specified sequencer counter if there is no recorded event" in {
        val env = factory.create(initialSequencerCounter = SequencerCounter(5))
        env.subscribeAfter().futureValue
        val counter = env.transport.subscriber.value.request.counter
        counter shouldBe SequencerCounter(5)
        env.client.close()
      }

      "starts subscription at last stored event (for fork verification)" in {
        val env = factory.create(storedEvents = Seq(deliver))
        env.subscribeAfter().futureValue
        val counter = env.transport.subscriber.value.request.counter
        counter shouldBe deliver.counter
        env.client.close()
      }

      "doesn't give prior event to the application handler" in {
        val validated = new AtomicBoolean()
        val processed = new AtomicBoolean()
        val env @ Env(_, transport, _, _, _) = factory.create(
          eventValidator = new SequencedEventValidator {
            override def validate(
                priorEvent: Option[PossiblyIgnoredSerializedEvent],
                event: OrdinarySerializedEvent,
                sequencerId: SequencerId,
            )(implicit
                traceContext: TraceContext
            ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit] = {
              validated.set(true)
              eventAlwaysValid.validate(priorEvent, event, sequencerId)
            }

            override def validateOnReconnect(
                priorEvent: Option[PossiblyIgnoredSerializedEvent],
                reconnectEvent: OrdinarySerializedEvent,
                sequencerId: SequencerId,
            )(implicit
                traceContext: TraceContext
            ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit] =
              validate(priorEvent, reconnectEvent, sequencerId)

            override def validatePekko[E: Pretty](
                subscription: SequencerSubscriptionPekko[E],
                priorReconnectEvent: Option[OrdinarySerializedEvent],
                sequencerId: SequencerId,
            )(implicit
                traceContext: TraceContext
            ): SequencerSubscriptionPekko[SequencedEventValidationError[E]] = {
              val SequencerSubscriptionPekko(source, health) =
                eventAlwaysValid.validatePekko(subscription, priorReconnectEvent, sequencerId)
              val observeValidation = source.map { x =>
                validated.set(true)
                x
              }
              SequencerSubscriptionPekko(observeValidation, health)
            }

            override def close(): Unit = ()
          },
          storedEvents = Seq(deliver),
        )

        val testF = for {
          _ <- env.subscribeAfter(
            deliver.timestamp,
            ApplicationHandler.create("") { events =>
              processed.set(true)
              alwaysSuccessfulHandler(events)
            },
          )
          _ = transport.subscriber.value.request.counter shouldBe deliver.counter
          _ <- transport.subscriber.value.sendToHandler(signedDeliver)
        } yield {
          eventually() {
            validated.get() shouldBe true
          }
          processed.get() shouldBe false
        }

        testF.futureValue
        env.client.close()
      }

      "picks the last prior event" in {
        val triggerNextDeliverHandling = new AtomicBoolean()
        val env = factory.create(storedEvents = Seq(deliver, nextDeliver, deliver44))
        val testF = for {
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
        env.client.close()
      }

      "replays messages from the SequencedEventStore" in {
        val processedEvents = new ConcurrentLinkedQueue[SequencerCounter]

        val env = factory.create(storedEvents = Seq(deliver, nextDeliver, deliver44))
        env
          .subscribeAfter(
            deliver.timestamp,
            ApplicationHandler.create("") { events =>
              events.value.foreach(event => processedEvents.add(event.counter))
              alwaysSuccessfulHandler(events)
            },
          )
          .futureValue

        processedEvents.iterator().asScala.toSeq shouldBe Seq(
          nextDeliver.counter,
          deliver44.counter,
        )
        env.client.close()
      }

      "propagates errors during replay" in {
        val syncError =
          ApplicationHandlerException(failureException, nextDeliver.counter, nextDeliver.counter)
        val syncExc = SequencerClientSubscriptionException(syncError)

        val env = factory.create(storedEvents = Seq(deliver, nextDeliver))

        loggerFactory.assertLogs(
          env.subscribeAfter(deliver.timestamp, alwaysFailingHandler).failed.futureValue,
          logEntry => {
            logEntry.errorMessage should include(
              "Synchronous event processing failed for event batch with sequencer counters 43 to 43"
            )
            logEntry.throwable shouldBe Some(failureException)
          },
          logEntry => {
            logEntry.errorMessage should include("Sequencer subscription failed")
            logEntry.throwable.value shouldBe syncExc
          },
        ) shouldBe syncExc
        env.client.close()
      }

      "throttle message batches" in {
        val counter = new AtomicInteger(0)
        val maxSeenCounter = new AtomicInteger(0)
        val maxSequencerCounter = new AtomicLong(0L)
        val env = factory.create(
          options = SequencerClientConfig(
            eventInboxSize = PositiveInt.tryCreate(1),
            maximumInFlightEventBatches = PositiveInt.tryCreate(5),
          ),
          initialSequencerCounter = SequencerCounter(1L),
        )

        env
          .subscribeAfter(
            CantonTimestamp.Epoch,
            ApplicationHandler.create("test-handler-throttling") { e =>
              val firstSc = e.value.head.counter
              val lastSc = e.value.last.counter
              logger.debug(s"Processing batch of events ${firstSc} to ${lastSc}")
              HandlerResult.asynchronous(
                FutureUnlessShutdown.outcomeF(Future {
                  blocking {
                    maxSeenCounter.synchronized {
                      maxSeenCounter.set(Math.max(counter.incrementAndGet(), maxSeenCounter.get()))
                    }
                  }
                  Threading.sleep(100)
                  counter.decrementAndGet().discard
                  maxSequencerCounter.updateAndGet(_ max lastSc.unwrap).discard
                }(SequencerClientTest.this.executorService))
              )
            },
          )
          .futureValue

        for (i <- 1 to 100) {
          env.transport.subscriber.value.sendToHandler(deliver(i.toLong)).futureValue
        }

        eventually() {
          maxSequencerCounter.get shouldBe 100
        }

        maxSeenCounter.get() shouldBe 5
        env.client.close()
      }
    }
  }

  def richSequencerClient(): Unit = {
    "subscribe" should {
      "stores the event in the SequencedEventStore" in {
        val env @ Env(client, transport, _, sequencedEventStore, _) = RichEnvFactory.create()
        val storedEventF = for {
          _ <- env.subscribeAfter()
          _ <- transport.subscriber.value.sendToHandler(signedDeliver)
          _ <- client.flush()
          storedEvent <- sequencedEventStore.sequencedEvents()
        } yield storedEvent

        storedEventF.futureValue shouldBe Seq(signedDeliver)
        env.client.close()
      }

      "stores the event even if the handler fails" in {
        val env @ Env(client, transport, _, sequencedEventStore, _) = RichEnvFactory.create()
        val storedEventF = for {
          _ <- env.subscribeAfter(eventHandler = alwaysFailingHandler)
          _ <- loggerFactory.assertLogs(
            {
              for {
                _ <- transport.subscriber.value.sendToHandler(signedDeliver)
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
        env.client.close()
      }

      "completes the sequencer client if the subscription closes due to an error" in {
        val error =
          EventValidationError(GapInSequencerCounter(SequencerCounter(666), SequencerCounter(0)))
        val env @ Env(client, transport, _, _, _) = RichEnvFactory.create()
        val closeReasonF = for {
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
        env.client.close()
      }

      "completes the sequencer client if the application handler fails" in {
        val error = new RuntimeException("failed handler")
        val syncError = ApplicationHandlerException(error, deliver.counter, deliver.counter)
        val handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
          ApplicationHandler.create("async-failure")(_ =>
            FutureUnlessShutdown.failed[AsyncResult](error)
          )

        val env @ Env(client, transport, _, _, _) = RichEnvFactory.create()
        val closeReasonF = for {
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

        val env @ Env(client, transport, _, _, _) = RichEnvFactory.create()
        val closeReasonF = for {
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

        val env @ Env(client, transport, _, _, _) = RichEnvFactory.create()
        val closeReasonF = for {
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

        val env @ Env(client, transport, _, _, _) = RichEnvFactory.create()
        val closeReasonF = for {
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
    }

    "subscribeTracking" should {
      "updates sequencer counter prehead" in {
        val Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) =
          RichEnvFactory.create()
        val preHeadF = for {
          _ <- client.subscribeTracking(
            sequencerCounterTrackerStore,
            alwaysSuccessfulHandler,
            timeTracker,
          )
          _ <- transport.subscriber.value.sendToHandler(signedDeliver)
          _ <- client.flushClean()
          preHead <- sequencerCounterTrackerStore.preheadSequencerCounter
        } yield preHead.value

        preHeadF.futureValue shouldBe CursorPrehead(deliver.counter, deliver.timestamp)
        client.close()
      }

      "replays from the sequencer counter prehead" in {
        val processedEvents = new ConcurrentLinkedQueue[SequencerCounter]
        val Env(client, _transport, sequencerCounterTrackerStore, _, timeTracker) =
          RichEnvFactory.create(
            storedEvents = Seq(deliver, nextDeliver, deliver44, deliver45),
            cleanPrehead = Some(CursorPrehead(nextDeliver.counter, nextDeliver.timestamp)),
          )
        val preheadF = for {
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
        client.close()
      }

      "resubscribes after replay" in {
        val processedEvents = new ConcurrentLinkedQueue[SequencerCounter]

        val Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) =
          RichEnvFactory.create(
            storedEvents = Seq(deliver, nextDeliver, deliver44),
            cleanPrehead = Some(CursorPrehead(nextDeliver.counter, nextDeliver.timestamp)),
          )
        val preheadF = for {
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
        client.close()
      }

      "does not update the prehead if the application handler fails" in {
        val Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) =
          RichEnvFactory.create()
        val preHeadF = for {
          _ <- client.subscribeTracking(
            sequencerCounterTrackerStore,
            alwaysFailingHandler,
            timeTracker,
          )
          _ <- loggerFactory.assertLogs(
            {
              for {
                _ <- transport.subscriber.value.sendToHandler(signedDeliver)
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
        client.close()
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

        val Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) =
          RichEnvFactory.create(
            options = SequencerClientConfig(eventInboxSize = PositiveInt.tryCreate(1))
          )
        val testF = for {
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
        client.close()
      }
    }

    "changeTransport" should {
      "create second subscription from the same counter as the previous one when there are no events" in {
        val secondTransport = MockTransport()
        val env = RichEnvFactory.create(initialSequencerCounter = SequencerCounter.Genesis)
        val testF = for {
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
        env.client.close()
      }

      "create second subscription from the same counter as the previous one when there are events" in {
        val secondTransport = MockTransport()

        val env = RichEnvFactory.create()
        val testF = for {
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
        val secondTransport = MockTransport()

        val env = RichEnvFactory.create()
        val testF = for {
          _ <- env.changeTransport(secondTransport)
          _ <- env.sendAsync(Batch.empty(testedProtocolVersion))
        } yield {
          env.transport.lastSend.get() shouldBe None
          secondTransport.lastSend.get() should not be None

          env.transport.isClosing shouldBe true
          secondTransport.isClosing shouldBe false
        }

        testF.futureValue
        env.client.close()
      }

      "have new transport be used for sends when there is subscription" in {
        val secondTransport = MockTransport()

        val env = RichEnvFactory.create()
        val testF = for {
          _ <- env.subscribeAfter()
          _ <- env.changeTransport(secondTransport)
          _ <- env.sendAsync(Batch.empty(testedProtocolVersion))
        } yield {
          env.transport.lastSend.get() shouldBe None
          secondTransport.lastSend.get() should not be None
        }

        testF.futureValue
        env.client.close()
      }

      "have new transport be used with same sequencerId but different sequencer alias" in {
        val secondTransport = MockTransport()

        val env = RichEnvFactory.create()
        val testF = for {
          _ <- env.subscribeAfter()
          _ <- env.changeTransport(
            SequencerTransports.single(
              SequencerAlias.tryCreate("somethingElse"),
              daSequencerId,
              secondTransport,
            )
          )
          _ <- env.sendAsync(Batch.empty(testedProtocolVersion))
        } yield {
          env.transport.lastSend.get() shouldBe None
          secondTransport.lastSend.get() should not be None
        }

        testF.futureValue
        env.client.close()
      }

      "fail to reassign sequencerId" in {
        val secondTransport = MockTransport()
        val secondSequencerId = SequencerId(
          UniqueIdentifier(Identifier.tryCreate("da2"), Namespace(Fingerprint.tryCreate("default")))
        )

        val env = RichEnvFactory.create()
        val testF = for {
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
        env.client.close()
      }
    }
  }

  "RichSequencerClientImpl" should {
    behave like sequencerClient(RichEnvFactory)
    behave like richSequencerClient()
  }

  "SequencerClientImplPekko" should {
    behave like sequencerClient(PekkoEnvFactory)
  }

  private sealed trait Subscriber[E] {
    def request: SubscriptionRequest
    def subscription: MockSubscription[E]
    def sendToHandler(event: OrdinarySerializedEvent): Future[Unit]

    def sendToHandler(event: SequencedEvent[ClosedEnvelope]): Future[Unit] = {
      sendToHandler(OrdinarySequencedEvent(SequencerTestUtils.sign(event), None)(traceContext))
    }
  }

  private case class OldStyleSubscriber[E](
      override val request: SubscriptionRequest,
      private val handler: SerializedEventHandler[E],
      override val subscription: MockSubscription[E],
  ) extends Subscriber[E] {
    override def sendToHandler(event: OrdinarySerializedEvent): Future[Unit] =
      handler(event).transform {
        case Success(Right(_)) => Success(())
        case Success(Left(err)) =>
          subscription.closeSubscription(err)
          Success(())
        case Failure(ex) =>
          subscription.closeSubscription(ex)
          Success(())
      }
  }

  private case class SubscriberPekko[E](
      override val request: SubscriptionRequest,
      private val queue: BoundedSourceQueue[OrdinarySerializedEvent],
      override val subscription: MockSubscription[E],
  ) extends Subscriber[E] {
    override def sendToHandler(event: OrdinarySerializedEvent): Future[Unit] = {
      queue.offer(event) match {
        case QueueOfferResult.Enqueued =>
          // TODO(#13789) This may need more synchronization
          Future.unit
        case QueueOfferResult.Failure(ex) =>
          logger.error(s"Failed to enqueue event", ex)
          fail("Failed to enqueue event")
        case other =>
          fail(s"Could not enqueue event $event: $other")
      }
    }
  }

  private case class Env[+Client <: SequencerClient](
      client: Client,
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

    def changeTransport(
        newTransport: SequencerClientTransport & SequencerClientTransportPekko
    )(implicit ev: Client <:< RichSequencerClient): Future[Unit] = {
      changeTransport(
        SequencerTransports.default(daSequencerId, newTransport)
      )
    }

    def changeTransport(sequencerTransports: SequencerTransports[?])(implicit
        ev: Client <:< RichSequencerClient
    ): Future[Unit] =
      ev(client).changeTransport(sequencerTransports)

    def sendAsync(
        batch: Batch[DefaultOpenEnvelope]
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncClientError, Unit] =
      client.sendAsync(batch).onShutdown(fail())
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

  private class MockTransport
      extends SequencerClientTransport
      with SequencerClientTransportPekko
      with NamedLogging {

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

      subscriber(retry = 100)
    }

    val lastSend = new AtomicReference[Option[SubmissionRequest]](None)

    override def acknowledgeSigned(request: SignedContent[AcknowledgeRequest])(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, Boolean] =
      EitherT.rightT(true)

    private def sendAsync(
        request: SubmissionRequest
    ): EitherT[Future, SendAsyncClientResponseError, Unit] = {
      lastSend.set(Some(request))
      EitherTUtil.unit
    }

    override def sendAsyncSigned(
        request: SignedContent[SubmissionRequest],
        timeout: Duration,
    )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientResponseError, Unit] =
      sendAsync(request.content)

    override def sendAsyncUnauthenticatedVersioned(
        request: SubmissionRequest,
        timeout: Duration,
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncClientResponseError, Unit] = ???

    override def subscribe[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(
        implicit traceContext: TraceContext
    ): SequencerSubscription[E] = {
      val subscription = new MockSubscription[E]

      if (
        !subscriberRef.compareAndSet(None, Some(OldStyleSubscriber(request, handler, subscription)))
      ) {
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

    override def downloadTopologyStateForInit(request: TopologyStateForInitRequest)(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, TopologyStateForInitResponse] = ???

    override type SubscriptionError = Uninhabited

    override def subscribe(request: SubscriptionRequest)(implicit
        traceContext: TraceContext
    ): SequencerSubscriptionPekko[SubscriptionError] = {
      // Choose a sufficiently large queue size so that we can test throttling
      val (queue, sourceQueue) =
        Source.queue[OrdinarySerializedEvent](200).preMaterialize()(materializer)

      val subscriber = SubscriberPekko(request, queue, new MockSubscription[Uninhabited]())
      subscriberRef.set(Some(subscriber))

      val source = sourceQueue
        .map(Either.right)
        .withUniqueKillSwitchMat()(Keep.right)
        .watchTermination()(Keep.both)

      SequencerSubscriptionPekko(
        source,
        new AlwaysHealthyComponent("sequencer-client-test-source", logger),
      )
    }

    override def subscribeUnauthenticated(request: SubscriptionRequest)(implicit
        traceContext: TraceContext
    ): SequencerSubscriptionPekko[SubscriptionError] = subscribe(request)

    override def subscriptionRetryPolicyPekko
        : SubscriptionErrorRetryPolicyPekko[SubscriptionError] =
      SubscriptionErrorRetryPolicyPekko.never
  }

  private object MockTransport {
    def apply(): MockTransport & SequencerClientTransportPekko.Aux[Uninhabited] = new MockTransport
  }

  private implicit class EnrichedSequencerClient(client: RichSequencerClient) {
    // flush needs to be called twice in order to finish asynchronous processing
    // (see comment around shutdown in SequencerClient). So we have this small
    // helper for the tests.
    def flushClean(): Future[Unit] = for {
      _ <- client.flush()
      _ <- client.flush()
    } yield ()
  }

  private val eventAlwaysValid: SequencedEventValidator = SequencedEventValidator.noValidation(
    DefaultTestIdentities.domainId,
    warn = false,
  )

  private trait EnvFactory[+Client <: SequencerClient] {
    def create(
        storedEvents: Seq[SequencedEvent[ClosedEnvelope]] = Seq.empty,
        cleanPrehead: Option[SequencerCounterCursorPrehead] = None,
        eventValidator: SequencedEventValidator = eventAlwaysValid,
        options: SequencerClientConfig = SequencerClientConfig(),
        initialSequencerCounter: SequencerCounter = firstSequencerCounter,
    )(implicit closeContext: CloseContext): Env[Client]

    protected def preloadStores(
        storedEvents: Seq[SequencedEvent[ClosedEnvelope]],
        cleanPrehead: Option[SequencerCounterCursorPrehead],
        sequencedEventStore: SequencedEventStore,
        sequencerCounterTrackerStore: SequencerCounterTrackerStore,
    ): Unit = {
      val signedEvents = storedEvents.map(SequencerTestUtils.sign)
      val preloadStores = for {
        _ <- sequencedEventStore.store(
          signedEvents.map(OrdinarySequencedEvent(_, None)(TraceContext.empty))
        )
        _ <- cleanPrehead.traverse_(prehead =>
          sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(prehead)
        )
      } yield ()
      preloadStores.futureValue
    }

    protected def maxRequestSizeLookup
        : DynamicDomainParametersLookup[DomainParametersLookup.SequencerDomainParameters] = {
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
        .thenReturn(Future.successful(TestDomainParameters.defaultDynamic))
      DomainParametersLookup.forSequencerDomainParameters(
        BaseTest.defaultStaticDomainParameters,
        None,
        topologyClient,
        FutureSupervisor.Noop,
        loggerFactory,
      )
    }

  }

  private object MockRequestSigner extends RequestSigner {
    override def signRequest[A <: HasCryptographicEvidence](
        request: A,
        hashPurpose: HashPurpose,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[Future, String, SignedContent[A]] = {
      val signedContent = SignedContent(
        request,
        SymbolicCrypto.emptySignature,
        None,
        testedProtocolVersion,
      )
      EitherT(Future.successful(Either.right[String, SignedContent[A]](signedContent)))
    }
  }

  private class ConstantSequencedEventValidatorFactory(eventValidator: SequencedEventValidator)
      extends SequencedEventValidatorFactory {
    override def create(
        unauthenticated: Boolean
    )(implicit loggingContext: NamedLoggingContext): SequencedEventValidator =
      eventValidator
  }

  private object RichEnvFactory extends EnvFactory[RichSequencerClient] {
    override def create(
        storedEvents: Seq[SequencedEvent[ClosedEnvelope]],
        cleanPrehead: Option[SequencerCounterCursorPrehead],
        eventValidator: SequencedEventValidator,
        options: SequencerClientConfig,
        initialSequencerCounter: SequencerCounter,
    )(implicit closeContext: CloseContext): Env[RichSequencerClient] = {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val timeouts = DefaultProcessingTimeouts.testing
      val transport = MockTransport()
      val sendTrackerStore = new InMemorySendTrackerStore()
      val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory)
      val sendTracker =
        new SendTracker(Map.empty, sendTrackerStore, metrics, loggerFactory, timeouts)
      val sequencerCounterTrackerStore =
        new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
      val timeTracker = new DomainTimeTracker(
        DomainTimeTrackerConfig(),
        clock,
        new MockTimeRequestSubmitter(),
        timeouts,
        loggerFactory,
      )
      val eventValidatorFactory = new ConstantSequencedEventValidatorFactory(eventValidator)

      val client = new RichSequencerClientImpl(
        DefaultTestIdentities.domainId,
        participant1,
        SequencerTransports.default(DefaultTestIdentities.daSequencerId, transport),
        options,
        TestingConfigInternal(),
        BaseTest.defaultStaticDomainParameters.protocolVersion,
        maxRequestSizeLookup,
        timeouts,
        eventValidatorFactory,
        clock,
        MockRequestSigner,
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
      )(parallelExecutionContext, tracer)

      preloadStores(storedEvents, cleanPrehead, sequencedEventStore, sequencerCounterTrackerStore)

      Env(client, transport, sequencerCounterTrackerStore, sequencedEventStore, timeTracker)
    }
  }

  private object PekkoEnvFactory extends EnvFactory[SequencerClient] {
    override def create(
        storedEvents: Seq[SequencedEvent[ClosedEnvelope]],
        cleanPrehead: Option[SequencerCounterCursorPrehead],
        eventValidator: SequencedEventValidator,
        options: SequencerClientConfig,
        initialSequencerCounter: SequencerCounter,
    )(implicit closeContext: CloseContext): Env[SequencerClient] = {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val timeouts = DefaultProcessingTimeouts.testing
      val transport = MockTransport()
      val sendTrackerStore = new InMemorySendTrackerStore()
      val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory)
      val sendTracker =
        new SendTracker(Map.empty, sendTrackerStore, metrics, loggerFactory, timeouts)
      val sequencerCounterTrackerStore =
        new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
      val timeTracker = new DomainTimeTracker(
        DomainTimeTrackerConfig(),
        clock,
        new MockTimeRequestSubmitter(),
        timeouts,
        loggerFactory,
      )
      val eventValidatorFactory = new ConstantSequencedEventValidatorFactory(eventValidator)

      val client = new SequencerClientImplPekko(
        DefaultTestIdentities.domainId,
        participant1,
        SequencerTransports.default(DefaultTestIdentities.daSequencerId, transport),
        options,
        TestingConfigInternal(),
        BaseTest.defaultStaticDomainParameters.protocolVersion,
        maxRequestSizeLookup,
        timeouts,
        eventValidatorFactory,
        clock,
        MockRequestSigner,
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
      )(PrettyInstances.prettyUninhabited, parallelExecutionContext, tracer, materializer)

      preloadStores(storedEvents, cleanPrehead, sequencedEventStore, sequencerCounterTrackerStore)

      Env(client, transport, sequencerCounterTrackerStore, sequencedEventStore, timeTracker)
    }
  }
}
