// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config as cantonConfig
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeInt,
  NonNegativeLong,
  Port,
  PositiveInt,
}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  Fingerprint,
  HashPurpose,
  SyncCryptoApi,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerPredecessor}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.HealthComponent.AlwaysHealthyComponent
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances}
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.metrics.{CommonMockMetrics, TrafficConsumptionMetrics}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.protocol.messages.{DefaultOpenEnvelope, UnsignedProtocolMessage}
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParametersLookup,
  StaticSynchronizerParameters,
  SynchronizerParametersLookup,
  TestSynchronizerParameters,
  v30,
}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.ConnectionX.ConnectionXConfig
import com.digitalasset.canton.sequencing.InternalSequencerConnectionX.{
  ConnectionAttributes,
  SequencerConnectionXHealth,
}
import com.digitalasset.canton.sequencing.SequencerConnectionXPool.{
  SequencerConnectionXPoolConfig,
  SequencerConnectionXPoolError,
}
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.SendAsyncClientResponseError
import com.digitalasset.canton.sequencing.client.SequencedEventValidationError.PreviousTimestampMismatch
import com.digitalasset.canton.sequencing.client.SequencerClient.CloseReason.{
  ClientShutdown,
  UnrecoverableError,
}
import com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.{
  ApplicationHandlerException,
  EventValidationError,
}
import com.digitalasset.canton.sequencing.client.SequencerClientTest.SentSubmission
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.{
  HandlerError,
  HandlerException,
}
import com.digitalasset.canton.sequencing.client.transports.{
  SequencerClientTransport,
  SequencerClientTransportPekko,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.{
  EventCostCalculator,
  TrafficConsumed,
  TrafficReceipt,
  TrafficStateController,
}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
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
import com.digitalasset.canton.time.{MockTimeRequestSubmitter, SimClock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  daSequencerId,
  mediatorId,
  participant1,
}
import com.digitalasset.canton.topology.client.{SynchronizerTopologyClient, TopologySnapshot}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.version.{
  IgnoreInSerializationTestExhaustivenessCheck,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import io.grpc.Status
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import org.scalatest
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.{Failure, Success}

final class SequencerClientTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with CloseableTest
    with BeforeAndAfterAll
    with ProtocolVersionChecksAnyWordSpec {

  private lazy val metrics = CommonMockMetrics.sequencerClient
  private lazy val deliver: Deliver[Nothing] =
    SequencerTestUtils.mockDeliver(
      CantonTimestamp.Epoch,
      synchronizerId = DefaultTestIdentities.physicalSynchronizerId,
    )
  private lazy val signedDeliver: SequencedEventWithTraceContext[ClosedEnvelope] =
    SequencedEventWithTraceContext(SequencerTestUtils.sign(deliver))(traceContext)

  private lazy val nextDeliver: Deliver[Nothing] = SequencerTestUtils.mockDeliver(
    timestamp = CantonTimestamp.ofEpochSecond(1),
    previousTimestamp = Some(CantonTimestamp.Epoch),
    synchronizerId = DefaultTestIdentities.physicalSynchronizerId,
  )
  private lazy val deliver44: Deliver[Nothing] = SequencerTestUtils.mockDeliver(
    timestamp = CantonTimestamp.ofEpochSecond(2),
    previousTimestamp = Some(CantonTimestamp.ofEpochSecond(1)),
    synchronizerId = DefaultTestIdentities.physicalSynchronizerId,
  )
  private lazy val deliver45: Deliver[Nothing] = SequencerTestUtils.mockDeliver(
    timestamp = CantonTimestamp.ofEpochSecond(3),
    previousTimestamp = Some(CantonTimestamp.ofEpochSecond(2)),
    synchronizerId = DefaultTestIdentities.physicalSynchronizerId,
  )

  private var actorSystem: ActorSystem = _
  private lazy val materializer: Materializer = Materializer(actorSystem)
  private lazy val topologyWithTrafficControl =
    TestingTopology(Set(DefaultTestIdentities.physicalSynchronizerId))
      .withDynamicSynchronizerParameters(
        DefaultTestIdentities.defaultDynamicSynchronizerParameters.tryUpdate(
          trafficControlParameters = Some(
            TrafficControlParameters(
              maxBaseTrafficAmount = NonNegativeLong.zero
            )
          )
        ),
        validFrom = CantonTimestamp.MinValue,
      )
      .build()
      .forOwnerAndSynchronizer(participant1)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    actorSystem = ActorSystem("SequencerClientTest")
  }

  override def afterAll(): Unit = {
    actorSystem.terminate().futureValue
    super.afterAll()
  }

  private def deliver(i: Long): Deliver[Nothing] = SequencerTestUtils.mockDeliver(
    timestamp = CantonTimestamp.Epoch.plusSeconds(i),
    previousTimestamp = if (i > 1) Some(CantonTimestamp.Epoch.plusSeconds(i - 1)) else None,
    DefaultTestIdentities.physicalSynchronizerId,
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
        env.subscribeAfter().futureValueUS

        val assertions: Seq[LogEntry => scalatest.Assertion] =
          if (env.useNewConnectionPool) {
            Seq { logEntry =>
              logEntry.errorMessage shouldBe "Sequencer subscription failed"
              logEntry.throwable.value.getMessage should include(
                "Post aggregation handler already set"
              )
            }
          } else {
            Seq(
              _.warningMessage shouldBe "Cannot create additional subscriptions to the sequencer from the same client",
              _.errorMessage should include("Sequencer subscription failed"),
            )
          }
        loggerFactory.assertLogs(
          env
            .subscribeAfter(CantonTimestamp.MinValue, alwaysSuccessfulHandler)
            .failed
            .futureValueUS,
          assertions*
        ) shouldBe a[RuntimeException]
        env.client.close()
      }

      "with no recorded events subscription should begin from None" in {
        val env = factory.create()
        env.subscribeAfter().futureValueUS
        val requestedTimestamp = env.subscriber.value.request.timestamp
        requestedTimestamp shouldBe None
      }

      "starts subscription at last stored event (for fork verification)" in {
        val env = factory.create(storedEvents = Seq(deliver))
        env.subscribeAfter().futureValueUS
        val startTimestamp = env.subscriber.value.request.timestamp
        startTimestamp shouldBe Some(deliver.timestamp)
        env.client.close()
      }

      "doesn't give prior event to the application handler" in {
        val validated = new AtomicBoolean()
        val processed = new AtomicBoolean()
        val env = factory.create(
          eventValidator = new SequencedEventValidator {
            override def validate(
                priorEvent: Option[ProcessingSerializedEvent],
                event: SequencedSerializedEvent,
                sequencerId: SequencerId,
            )(implicit
                traceContext: TraceContext
            ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit] = {
              validated.set(true)
              eventAlwaysValid.validate(priorEvent, event, sequencerId)
            }

            override def validateOnReconnect(
                priorEvent: Option[ProcessingSerializedEvent],
                reconnectEvent: SequencedSerializedEvent,
                sequencerId: SequencerId,
            )(implicit
                traceContext: TraceContext
            ): EitherT[FutureUnlessShutdown, SequencedEventValidationError[Nothing], Unit] =
              validate(priorEvent, reconnectEvent, sequencerId)

            override def validatePekko[E: Pretty](
                subscription: SequencerSubscriptionPekko[E],
                priorReconnectEvent: Option[SequencedSerializedEvent],
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
          _ = env.subscriber.value.request.timestamp shouldBe Some(deliver.timestamp)
          _ <- env.subscriber.value.sendToHandler(signedDeliver)
        } yield {
          eventually() {
            validated.get() shouldBe true
          }
          processed.get() shouldBe false
        }

        testF.futureValueUS
        env.client.close()
      }

      "picks the last prior event" in {
        val triggerNextDeliverHandling = new AtomicBoolean()
        val env = factory.create(storedEvents = Seq(deliver, nextDeliver, deliver44))
        val testF = for {
          _ <- env.subscribeAfter(
            nextDeliver.timestamp.immediatePredecessor,
            ApplicationHandler.create("") { events =>
              if (events.value.exists(_.timestamp == nextDeliver.timestamp)) {
                triggerNextDeliverHandling.set(true)
              }
              HandlerResult.done
            },
          )
        } yield ()

        testF.futureValueUS
        triggerNextDeliverHandling.get shouldBe true
        env.client.close()
      }

      "replays messages from the SequencedEventStore" in {
        val processedEvents = new ConcurrentLinkedQueue[CantonTimestamp]

        val env = factory.create(storedEvents = Seq(deliver, nextDeliver, deliver44))
        env
          .subscribeAfter(
            deliver.timestamp,
            ApplicationHandler.create("") { events =>
              events.value.foreach(event => processedEvents.add(event.timestamp))
              alwaysSuccessfulHandler(events)
            },
          )
          .futureValueUS

        processedEvents.iterator().asScala.toSeq shouldBe Seq(
          nextDeliver.timestamp,
          deliver44.timestamp,
        )
        env.client.close()
      }

      "propagates errors during replay" in {
        val syncError =
          ApplicationHandlerException(
            failureException,
            nextDeliver.timestamp,
            nextDeliver.timestamp,
          )
        val syncExc = SequencerClientSubscriptionException(syncError)

        val env = factory.create(storedEvents = Seq(deliver, nextDeliver))

        loggerFactory.assertLogs(
          env.subscribeAfter(deliver.timestamp, alwaysFailingHandler).failed.futureValueUS,
          logEntry => {
            logEntry.errorMessage should include(
              s"Synchronous event processing failed for event batch with sequencing timestamps ${nextDeliver.timestamp} to ${nextDeliver.timestamp}"
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
          initializeCounterAllocatorTo = Some(SequencerCounter(0)),
          options = SequencerClientConfig(
            eventInboxSize = PositiveInt.tryCreate(1),
            maximumInFlightEventBatches = PositiveInt.tryCreate(5),
          ),
        )

        env
          .subscribeAfter(
            CantonTimestamp.Epoch,
            ApplicationHandler.create("test-handler-throttling") { e =>
              val firstSc = e.value.head.counter
              val lastSc = e.value.last.counter
              logger.debug(s"Processing batch of events $firstSc to $lastSc")
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
          .futureValueUS

        for (i <- 1 to 100) {
          env.subscriber.value.sendToHandler(deliver(i.toLong)).futureValueUS
        }

        eventually() {
          maxSequencerCounter.get shouldBe 100
        }

        maxSeenCounter.get() shouldBe 5
        env.client.close()
      }

      "time limit the synchronous application handler" in {
        val env = factory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41)),
          storedEvents = Seq(deliver, nextDeliver, deliver44),
        )
        val promise = Promise[AsyncResult[Unit]]()

        val testF = loggerFactory.assertLogs(
          env.subscribeAfter(
            nextDeliver.timestamp.immediatePredecessor,
            ApplicationHandler.create("long running synchronous handler") { _ =>
              env.clock.advance(
                java.time.Duration.of(
                  DefaultProcessingTimeouts.testing.sequencedEventProcessingBound.asFiniteApproximation.toNanos,
                  ChronoUnit.NANOS,
                )
              )
              FutureUnlessShutdown.outcomeF(promise.future)
            },
          ),
          _.errorMessage should include(
            "Processing of event batch with sequencing timestamps 1970-01-01T00:00:01Z to 1970-01-01T00:00:02Z started at 1970-01-01T00:00:00Z did not complete by 1970-01-02T00:00:00Z"
          ),
        )

        // After the timeout has been logged as an error, complete the application handler so that the test can shut down gracefully.
        promise.success(AsyncResult.immediate)
        testF.futureValueUS
        env.client.close()
      }

      "time limit the asynchronous application handler" in {
        val env = factory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41)),
          storedEvents = Seq(deliver, nextDeliver, deliver44),
        )
        val promise = Promise[Unit]()

        val testF = loggerFactory.assertLogs(
          env.subscribeAfter(
            nextDeliver.timestamp.immediatePredecessor,
            ApplicationHandler.create("long running asynchronous handler") { _ =>
              env.clock.advance(
                java.time.Duration.of(
                  DefaultProcessingTimeouts.testing.sequencedEventProcessingBound.asFiniteApproximation.toNanos,
                  ChronoUnit.NANOS,
                )
              )
              HandlerResult.asynchronous(FutureUnlessShutdown.outcomeF(promise.future))
            },
          ),
          _.errorMessage should include(
            "Processing of event batch with sequencing timestamps 1970-01-01T00:00:01Z to 1970-01-01T00:00:02Z started at 1970-01-01T00:00:00Z did not complete by 1970-01-02T00:00:00Z"
          ),
        )

        // After the timeout has been logged as an error, complete the application handler so that the test can shut down gracefully.
        promise.success(())
        testF.futureValueUS
        env.client.close()
      }
    }
  }

  def richSequencerClient(): Unit = {
    "subscribe" should {
      "stores the event in the SequencedEventStore" in {
        val env = RichEnvFactory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41))
        )
        import env.*
        val storedEventF = for {
          _ <- env.subscribeAfter()
          _ <- subscriber.value.sendToHandler(signedDeliver)
          _ <- client.flush()
          storedEvent <- sequencedEventStore.sequencedEvents()
        } yield storedEvent

        storedEventF.futureValueUS shouldBe Seq(
          signedDeliver.asOrdinaryEvent(counter = SequencerCounter(42))
        )
        env.client.close()
      }

      "stores the event even if the handler fails" in {
        val env = RichEnvFactory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41))
        )
        import env.*
        val storedEventF = for {
          _ <- env.subscribeAfter(eventHandler = alwaysFailingHandler)
          _ <- loggerFactory.assertLogs(
            for {
              _ <- subscriber.value.sendToHandler(signedDeliver)
              _ <- client.flush()
            } yield (),
            logEntry => {
              logEntry.errorMessage should be(
                "Synchronous event processing failed for event batch with sequencing timestamps 1970-01-01T00:00:00Z to 1970-01-01T00:00:00Z."
              )
              logEntry.throwable.value shouldBe failureException
            },
          )
          storedEvent <- sequencedEventStore.sequencedEvents()
        } yield storedEvent

        storedEventF.futureValueUS shouldBe Seq(
          signedDeliver.asOrdinaryEvent(counter = SequencerCounter(42))
        )
        env.client.close()
      }

      "completes the sequencer client if the subscription closes due to an error" in {
        val error =
          EventValidationError(
            PreviousTimestampMismatch(
              receivedPreviousTimestamp = Some(CantonTimestamp.ofEpochSecond(666)),
              expectedPreviousTimestamp = Some(CantonTimestamp.Epoch),
            )
          )
        val env = RichEnvFactory.create()
        import env.*
        val closeReasonF = for {
          _ <- env.subscribeAfter(CantonTimestamp.MinValue, alwaysSuccessfulHandler)
          subscription = subscriber
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

        closeReasonF.futureValueUS should matchPattern {
          case e: UnrecoverableError if e.cause == s"handler returned error: $error" =>
        }
        env.client.close()
      }

      "completes the sequencer client if the application handler fails" in {
        val error = new RuntimeException("failed handler")
        val syncError = ApplicationHandlerException(error, deliver.timestamp, deliver.timestamp)
        val handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
          ApplicationHandler.create("async-failure")(_ =>
            FutureUnlessShutdown.failed[AsyncResult[Unit]](error)
          )

        val env = RichEnvFactory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41))
        )
        import env.*
        val closeReasonF = for {
          _ <- env.subscribeAfter(CantonTimestamp.MinValue, handler)
          closeReason <- loggerFactory.assertLogs(
            for {
              _ <- subscriber.value.sendToHandler(deliver)
              // Send the next event so that the client notices that an error has occurred.
              _ <- client.flush()
              _ <- subscriber.value.sendToHandler(nextDeliver)
              // wait until the subscription is closed (will emit an error)
              closeReason <- client.completion
            } yield closeReason,
            logEntry => {
              logEntry.errorMessage should be(
                s"Synchronous event processing failed for event batch with sequencing timestamps ${deliver.timestamp} to ${deliver.timestamp}."
              )
              logEntry.throwable shouldBe Some(error)
            },
            _.errorMessage should include(
              if (env.useNewConnectionPool)
                s"Permanently closing sequencer subscription due to handler exception (this indicates a bug): $syncError"
              else
                s"Sequencer subscription is being closed due to handler exception (this indicates a bug): $syncError"
            ),
          )

        } yield {
          client.close() // make sure that we can still close the sequencer client
          closeReason
        }

        closeReasonF.futureValueUS should matchPattern {
          case e: UnrecoverableError if e.cause == s"handler returned error: $syncError" =>
        }
      }

      "completes the sequencer client if the application handler shuts down synchronously" in {
        val handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
          ApplicationHandler.create("shutdown")(_ => FutureUnlessShutdown.abortedDueToShutdown)

        val env = RichEnvFactory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41))
        )
        import env.*
        val closeReasonF = for {
          _ <- env.subscribeAfter(eventHandler = handler)
          closeReason <- {
            for {
              _ <- subscriber.value.sendToHandler(deliver)
              // Send the next event so that the client notices that an error has occurred.
              _ <- client.flush()
              _ <- subscriber.value.sendToHandler(nextDeliver)
              closeReason <- client.completion
            } yield closeReason
          }
        } yield {
          client.close() // make sure that we can still close the sequencer client
          closeReason
        }

        closeReasonF.futureValueUS shouldBe ClientShutdown
      }

      "completes the sequencer client if asynchronous event processing fails" in {
        val error = new RuntimeException("asynchronous failure")
        val asyncFailure = HandlerResult.asynchronous(FutureUnlessShutdown.failed(error))
        val asyncException =
          ApplicationHandlerException(error, deliver.timestamp, deliver.timestamp)

        val env = RichEnvFactory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41))
        )
        import env.*
        val closeReasonF = for {
          _ <- env.subscribeAfter(
            eventHandler = ApplicationHandler.create("async-failure")(_ => asyncFailure)
          )
          closeReason <- loggerFactory.assertLogs(
            for {
              _ <- subscriber.value.sendToHandler(deliver)
              // Make sure that the asynchronous error has been noticed
              // We intentionally do two flushes. The first captures `handleReceivedEventsUntilEmpty` completing.
              // During this it may addToFlush a future for capturing `asyncSignalledF` however this may occur
              // after we've called `flush` and therefore won't guarantee completing all processing.
              // So our second flush will capture `asyncSignalledF` for sure.
              _ <- client.flush()
              _ <- client.flush()
              // Send the next event so that the client notices that an error has occurred.
              _ <- subscriber.value.sendToHandler(nextDeliver)
              _ <- client.flush()
              // wait until client completed (will write an error)
              closeReason <- client.completion
              _ = client.close() // make sure that we can still close the sequencer client
            } yield closeReason,
            logEntry => {
              logEntry.errorMessage should include(
                s"Asynchronous event processing failed for event batch with sequencing timestamps ${deliver.timestamp} to ${deliver.timestamp}"
              )
              logEntry.throwable shouldBe Some(error)
            },
            _.errorMessage should include(
              if (env.useNewConnectionPool)
                s"Permanently closing sequencer subscription due to handler exception (this indicates a bug): $asyncException"
              else
                s"Sequencer subscription is being closed due to handler exception (this indicates a bug): $asyncException"
            ),
          )
        } yield closeReason

        closeReasonF.futureValueUS should matchPattern {
          case e: UnrecoverableError if e.cause == s"handler returned error: $asyncException" =>
        }
      }

      "completes the sequencer client if asynchronous event processing shuts down" in {
        val asyncShutdown = HandlerResult.asynchronous(FutureUnlessShutdown.abortedDueToShutdown)

        val env = RichEnvFactory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41))
        )
        import env.*
        val closeReasonF = for {
          _ <- env.subscribeAfter(
            CantonTimestamp.MinValue,
            ApplicationHandler.create("async-shutdown")(_ => asyncShutdown),
          )
          closeReason <- {
            for {
              _ <- subscriber.value.sendToHandler(deliver)
              _ <- client.flushClean() // Make sure that the asynchronous error has been noticed
              // Send the next event so that the client notices that an error has occurred.
              _ <- subscriber.value.sendToHandler(nextDeliver)
              _ <- client.flush()
              closeReason <- client.completion
            } yield closeReason
          }
        } yield {
          client.close() // make sure that we can still close the sequencer client
          closeReason
        }

        closeReasonF.futureValueUS shouldBe ClientShutdown
      }

      "invokes exit on fatal error handler due to a fatal error" in {
        val error =
          EventValidationError(
            PreviousTimestampMismatch(
              receivedPreviousTimestamp = Some(CantonTimestamp.ofEpochSecond(665)),
              expectedPreviousTimestamp = Some(CantonTimestamp.ofEpochSecond(666)),
            )
          )

        var errorReport: String = "not reported"
        def mockExitOnFatalError(message: String, logger: TracedLogger)(
            traceContext: TraceContext
        ): Unit = {
          logger.info(s"Reporting mock fatal/exit error $message")(traceContext)
          errorReport = message
        }

        val env = RichEnvFactory.create(mockExitOnFatalErrorO = Some(mockExitOnFatalError))
        import env.*
        val closeReasonF = for {
          _ <- env.subscribeAfter(CantonTimestamp.MinValue, alwaysSuccessfulHandler)
          subscription = subscriber
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

        closeReasonF.futureValueUS should matchPattern {
          case e: UnrecoverableError if e.cause == s"handler returned error: $error" =>
        }
        env.client.close()
        // The connection pool, unlike the ResilientSequencerSubscription, does not use a `maybeExitOnFatalError`
        // (see `maybeExitOnFatalError` in SequencerClient).
        if (!env.useNewConnectionPool)
          errorReport shouldBe "Sequenced timestamp mismatch received Some(1970-01-01T00:11:05Z) but expected Some(1970-01-01T00:11:06Z). Has there been a TransportChange?"
      }

      "acknowledgeSigned should take upgrade time into account" in {
        def test(usePredecessor: Boolean) = {
          val psid = DefaultTestIdentities.physicalSynchronizerId
          val nextPSid = PhysicalSynchronizerId(
            psid.logical,
            psid.protocolVersion,
            psid.serial.increment.toNonNegative,
          )

          val upgradeTime = CantonTimestamp.now()
          val synchronizerPredecessor =
            Option.when(usePredecessor)(SynchronizerPredecessor(psid, upgradeTime))

          val env = RichEnvFactory.create(
            psid = nextPSid,
            synchronizerPredecessor = synchronizerPredecessor,
          )

          val res = for {
            _ <- env.client.acknowledgeSigned(upgradeTime.immediatePredecessor).value
            _ <- env.client.acknowledgeSigned(upgradeTime).value
            _ <- env.client.acknowledgeSigned(upgradeTime.immediateSuccessor).value

            expectedAcknowledgedTimestamps =
              if (usePredecessor) {
                // The predecessor and upgradeTime are not acknowledged because it is before the upgraded (so belongs to old synchronizer)
                Set(upgradeTime.immediateSuccessor)
              } else
                Set(upgradeTime.immediatePredecessor, upgradeTime, upgradeTime.immediateSuccessor)

            acknowledgedTimestamps =
              if (env.useNewConnectionPool) env.pool.acknowledgedTimestamps.get
              else env.transport.acknowledgedTimestamps.get

            _ = acknowledgedTimestamps shouldBe expectedAcknowledgedTimestamps

          } yield ()

          res.futureValueUS

          env.client.close()
        }

        test(usePredecessor = false)
        test(usePredecessor = true)
      }
    }

    "subscribeTracking" should {
      "updates sequencer counter prehead" in {
        val env = RichEnvFactory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41))
        )
        import env.*
        val preHeadF = for {
          _ <- client.subscribeTracking(
            sequencerCounterTrackerStore,
            alwaysSuccessfulHandler,
            timeTracker,
          )
          _ <- subscriber.value.sendToHandler(signedDeliver)
          _ <- client.flushClean()
          preHead <- sequencerCounterTrackerStore.preheadSequencerCounter
        } yield preHead.value

        preHeadF.futureValueUS shouldBe CursorPrehead(SequencerCounter(42), deliver.timestamp)
        client.close()
      }

      "replays from the sequencer counter prehead" in {
        val processedEvents = new ConcurrentLinkedQueue[SequencerCounter]
        val env = RichEnvFactory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41)),
          storedEvents = Seq(deliver, nextDeliver, deliver44, deliver45),
          cleanPrehead = Some(CursorPrehead(SequencerCounter(43), nextDeliver.timestamp)),
        )
        import env.*
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
          prehead <-
            sequencerCounterTrackerStore.preheadSequencerCounter
        } yield prehead.value

        preheadF.futureValueUS shouldBe CursorPrehead(SequencerCounter(45), deliver45.timestamp)
        processedEvents.iterator().asScala.toSeq shouldBe Seq(
          SequencerCounter(44),
          SequencerCounter(45),
        )
        client.close()
      }

      "resubscribes after replay" in {
        val processedEvents = new ConcurrentLinkedQueue[SequencerCounter]

        val env = RichEnvFactory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41)),
          storedEvents = Seq(deliver, nextDeliver, deliver44),
          cleanPrehead = Some(CursorPrehead(SequencerCounter(43), nextDeliver.timestamp)),
        )
        import env.*
        val preheadF = for {
          _ <- client.subscribeTracking(
            sequencerCounterTrackerStore,
            ApplicationHandler.create("") { events =>
              events.value.foreach(event => processedEvents.add(event.counter))
              alwaysSuccessfulHandler(events)
            },
            timeTracker,
          )
          _ <- subscriber.value.sendToHandler(deliver45)
          _ <- client.flushClean()
          prehead <- sequencerCounterTrackerStore.preheadSequencerCounter
        } yield prehead.value

        preheadF.futureValueUS shouldBe CursorPrehead(SequencerCounter(45), deliver45.timestamp)

        processedEvents.iterator().asScala.toSeq shouldBe Seq(
          SequencerCounter(44),
          SequencerCounter(45),
        )
        client.close()
      }

      "does not update the prehead if the application handler fails" in {
        val env = RichEnvFactory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41))
        )
        import env.*
        val preHeadF = for {
          _ <- client.subscribeTracking(
            sequencerCounterTrackerStore,
            alwaysFailingHandler,
            timeTracker,
          )
          _ <- loggerFactory.assertLogs(
            for {
              _ <- subscriber.value.sendToHandler(signedDeliver)
              _ <- client.flushClean()
            } yield (),
            logEntry => {
              logEntry.errorMessage should be(
                "Synchronous event processing failed for event batch with sequencing timestamps 1970-01-01T00:00:00Z to 1970-01-01T00:00:00Z."
              )
              logEntry.throwable.value shouldBe failureException
            },
          )
          preHead <- sequencerCounterTrackerStore.preheadSequencerCounter
        } yield preHead

        preHeadF.futureValueUS shouldBe None
        client.close()
      }

      "updates the prehead only after the asynchronous processing has been completed" in {
        val promises = Map[SequencerCounter, Promise[UnlessShutdown[Unit]]](
          SequencerCounter(43) -> Promise[UnlessShutdown[Unit]](),
          SequencerCounter(44) -> Promise[UnlessShutdown[Unit]](),
        )

        def handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
          ApplicationHandler.create("") { events =>
            assert(events.value.sizeIs == 1)
            promises.get(events.value(0).counter) match {
              case None => HandlerResult.done
              case Some(promise) => HandlerResult.asynchronous(FutureUnlessShutdown(promise.future))
            }
          }

        val env = RichEnvFactory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41)),
          options = SequencerClientConfig(eventInboxSize = PositiveInt.tryCreate(1)),
        )
        import env.*
        val testF = for {
          _ <- client.subscribeTracking(sequencerCounterTrackerStore, handler, timeTracker)
          _ <- subscriber.value.sendToHandler(deliver)
          _ <- client.flushClean()
          prehead42 <-
            sequencerCounterTrackerStore.preheadSequencerCounter
          _ <- subscriber.value.sendToHandler(nextDeliver)
          prehead43 <-
            sequencerCounterTrackerStore.preheadSequencerCounter
          _ <- subscriber.value.sendToHandler(deliver44)
          _ = promises(SequencerCounter(44)).success(UnlessShutdown.unit)
          prehead43a <-
            sequencerCounterTrackerStore.preheadSequencerCounter
          _ = promises(SequencerCounter(43)).success(
            UnlessShutdown.unit
          ) // now we can advance the prehead
          _ <- client.flushClean()
          prehead44 <-
            sequencerCounterTrackerStore.preheadSequencerCounter
        } yield {
          prehead42 shouldBe Some(CursorPrehead(SequencerCounter(42), deliver.timestamp))
          prehead43 shouldBe Some(CursorPrehead(SequencerCounter(42), deliver.timestamp))
          prehead43a shouldBe Some(CursorPrehead(SequencerCounter(42), deliver.timestamp))
          prehead44 shouldBe Some(CursorPrehead(SequencerCounter(44), deliver44.timestamp))
        }

        testF.futureValueUS
        client.close()
      }
    }

    "submissionCost" should {
      "compute submission cost and update traffic state when receiving the receipt" in {
        val env = RichEnvFactory.create(
          topologyO = Some(topologyWithTrafficControl)
        )
        val messageId = MessageId.tryCreate("mock-deliver")
        val trafficReceipt = TrafficReceipt(
          consumedCost = NonNegativeLong.tryCreate(4),
          extraTrafficConsumed = NonNegativeLong.tryCreate(4),
          baseTrafficRemainder = NonNegativeLong.zero,
        )
        val testF = for {
          _ <- env.subscribeAfter()
          _ <- env
            .sendAsync(
              Batch.of(
                testedProtocolVersion,
                (new TestProtocolMessage(), Recipients.cc(participant1)),
              ),
              messageId = messageId,
            )
            .value
          _ <- env.subscriber.value.sendToHandler(
            SequencedEventWithTraceContext(
              SequencerTestUtils.sign(
                SequencerTestUtils.mockDeliver(
                  CantonTimestamp.MinValue.immediateSuccessor,
                  synchronizerId = DefaultTestIdentities.physicalSynchronizerId,
                  messageId = Some(messageId),
                  trafficReceipt = Some(trafficReceipt),
                )
              )
            )(
              traceContext
            )
          )
          _ <- env.client.flushClean()
        } yield {
          env.trafficStateController.getTrafficConsumed shouldBe TrafficConsumed(
            mediatorId,
            CantonTimestamp.MinValue.immediateSuccessor,
            trafficReceipt.extraTrafficConsumed,
            trafficReceipt.baseTrafficRemainder,
            trafficReceipt.consumedCost,
          )
        }

        testF.futureValueUS
        env.client.close()
      }

      "consume traffic from deliver errors" in {
        val env = RichEnvFactory.create(
          topologyO = Some(topologyWithTrafficControl)
        )
        val messageId = MessageId.tryCreate("mock-deliver")
        val trafficReceipt = TrafficReceipt(
          NonNegativeLong.tryCreate(4),
          NonNegativeLong.tryCreate(4),
          NonNegativeLong.zero,
        )
        val testF = for {
          _ <- env.subscribeAfter()
          _ <- env
            .sendAsync(
              Batch.of(
                testedProtocolVersion,
                (new TestProtocolMessage(), Recipients.cc(participant1)),
              ),
              messageId = messageId,
            )
            .value
          _ <- env.subscriber.value.sendToHandler(
            SequencedEventWithTraceContext(
              SequencerTestUtils.sign(
                SequencerTestUtils.mockDeliverError(
                  CantonTimestamp.MinValue.immediateSuccessor,
                  DefaultTestIdentities.physicalSynchronizerId,
                  messageId = messageId,
                  trafficReceipt = Some(trafficReceipt),
                )
              )
            )(
              traceContext
            )
          )
          _ <- env.client.flushClean()
        } yield {
          env.trafficStateController.getTrafficConsumed shouldBe TrafficConsumed(
            mediatorId,
            CantonTimestamp.MinValue.immediateSuccessor,
            trafficReceipt.extraTrafficConsumed,
            trafficReceipt.baseTrafficRemainder,
            trafficReceipt.consumedCost,
          )
        }

        testF.futureValueUS
        env.client.close()
      }
    }

    "a synchronous error" should {
      val amplificationConfig = SubmissionRequestAmplification(
        factor = PositiveInt.two,
        patience = config.NonNegativeFiniteDuration.Zero,
      )

      "trigger amplification if it is 'overloaded' and the flag is set" in {
        val env = RichEnvFactory.create(
          options = SequencerClientConfig(enableAmplificationImprovements = true),
          amplificationConfig = amplificationConfig,
          firstSendAsyncResponseO = Some(Left(overloadedError)),
        )

        env
          .sendAsync(Batch.empty(testedProtocolVersion), amplify = true)
          .futureValueUS shouldBe Right(())

        env.client.close()
      }

      "not trigger amplification if it is 'overloaded' and the flag is not set" in {
        val env = RichEnvFactory.create(
          options = SequencerClientConfig(enableAmplificationImprovements = false),
          amplificationConfig = amplificationConfig,
          firstSendAsyncResponseO = Some(Left(overloadedError)),
        )

        env
          .sendAsync(Batch.empty(testedProtocolVersion), amplify = true)
          .futureValueUS shouldBe Left(overloadedError)

        env.client.close()
      }

      "not trigger amplification if it is not 'overloaded' and the flag is set" in {
        val env = RichEnvFactory.create(
          options = SequencerClientConfig(enableAmplificationImprovements = true),
          amplificationConfig = amplificationConfig,
          firstSendAsyncResponseO = Some(Left(senderUnknownError)),
        )

        env
          .sendAsync(Batch.empty(testedProtocolVersion), amplify = true)
          .futureValueUS shouldBe Left(senderUnknownError)

        env.client.close()
      }
    }

    "amplification" should {
      val amplificationConfig = SubmissionRequestAmplification(
        factor = PositiveInt.two,
        patience = config.NonNegativeFiniteDuration.tryFromDuration(10.seconds),
      )

      def lastSubmissionTime(env: Env[?]): CantonTimestamp =
        if (env.useNewConnectionPool) env.pool.connection.lastSubmissionTime.value
        else env.transport.lastSubmissionTime.value

      def checkTimeOfAmplification(env: Env[?], expected: CantonTimestamp): Assertion = {
        env.clock.advanceTo(expected.immediatePredecessor)
        // Not yet sent...
        lastSubmissionTime(env) shouldBe CantonTimestamp.Epoch

        env.clock.advanceTo(expected)
        // Now it is
        eventually() { // The scheduling might happen asynchronously
          lastSubmissionTime(env) shouldBe expected
        }
      }

      "be scheduled after the transport delay if the flag is not set" in {
        val env = RichEnvFactory.create(
          options = SequencerClientConfig(enableAmplificationImprovements = false),
          amplificationConfig = amplificationConfig,
          firstSendAsyncResponseO = Some(Right(_.advance(3.seconds.toJava))),
        )

        env
          .sendAsync(Batch.empty(testedProtocolVersion), amplify = true)
          .futureValueUS shouldBe Right(())

        checkTimeOfAmplification(env, CantonTimestamp.ofEpochSecond(13))

        env.client.close()
      }

      "take into account the transport delay if the flag is set" in {
        val env = RichEnvFactory.create(
          options = SequencerClientConfig(enableAmplificationImprovements = true),
          amplificationConfig = amplificationConfig,
          firstSendAsyncResponseO = Some(Right(_.advance(3.seconds.toJava))),
        )

        env
          .sendAsync(Batch.empty(testedProtocolVersion), amplify = true)
          .futureValueUS shouldBe Right(())

        checkTimeOfAmplification(env, CantonTimestamp.ofEpochSecond(10))

        env.client.close()
      }

      "be scheduled after the transport delay if it exceeds the patience and the flag is not set" in {
        val env = RichEnvFactory.create(
          options = SequencerClientConfig(enableAmplificationImprovements = false),
          amplificationConfig = amplificationConfig,
          firstSendAsyncResponseO = Some(Right(_.advance(13.seconds.toJava))),
        )

        env
          .sendAsync(Batch.empty(testedProtocolVersion), amplify = true)
          .futureValueUS shouldBe Right(())

        // Amplification did not yet take place
        lastSubmissionTime(env) shouldBe CantonTimestamp.ofEpochSecond(0)

        checkTimeOfAmplification(env, CantonTimestamp.ofEpochSecond(23))

        env.client.close()
      }

      "be scheduled immediately if the transport delay exceeds the patience and the flag is set" in {
        val env = RichEnvFactory.create(
          options = SequencerClientConfig(enableAmplificationImprovements = true),
          amplificationConfig = amplificationConfig,
          firstSendAsyncResponseO = Some(Right(_.advance(13.seconds.toJava))),
        )

        env
          .sendAsync(Batch.empty(testedProtocolVersion), amplify = true)
          .futureValueUS shouldBe Right(())

        // Amplification already took place
        eventually() { // The scheduling might happen asynchronously
          lastSubmissionTime(env) shouldBe CantonTimestamp.ofEpochSecond(13)
        }

        env.client.close()
      }
    }

    "changeTransport" should {
      "create second subscription from the same counter as the previous one when there are no events" in {
        // TODO(i26481): Enable new connection pool (test uses changeTransport())
        val env = RichEnvFactory.create(useNewConnectionPoolO = Some(false))
        val secondTransport = MockTransport(env.clock)

        val testF = for {
          _ <- env.subscribeAfter()
          _ <- env.changeTransport(secondTransport, None)
        } yield {
          val originalSubscriber = env.transport.subscriber.value
          originalSubscriber.request.timestamp shouldBe None
          originalSubscriber.subscription.isClosing shouldBe true // old subscription gets closed
          env.transport.isClosing shouldBe true

          val newSubscriber = secondTransport.subscriber.value
          newSubscriber.request.timestamp shouldBe None
          newSubscriber.subscription.isClosing shouldBe false
          secondTransport.isClosing shouldBe false

          env.client.completion.isCompleted shouldBe false
        }

        testF.futureValueUS
        env.client.close()
      }

      "create second subscription from the same counter as the previous one when there are events" in {
        // TODO(i26481): Enable new connection pool (test uses changeTransport())
        val env = RichEnvFactory.create(
          initializeCounterAllocatorTo = Some(SequencerCounter(41)),
          useNewConnectionPoolO = Some(false),
        )
        val secondTransport = MockTransport(env.clock)

        val testF = for {
          _ <- env.subscribeAfter()

          _ <- env.transport.subscriber.value.sendToHandler(deliver)
          _ <- env.transport.subscriber.value.sendToHandler(nextDeliver)
          _ <- env.client.flushClean()

          _ <- env.changeTransport(secondTransport, None)
        } yield {
          val originalSubscriber = env.transport.subscriber.value
          originalSubscriber.request.timestamp shouldBe None

          val newSubscriber = secondTransport.subscriber.value
          newSubscriber.request.timestamp shouldBe Some(nextDeliver.timestamp)

          env.client.completion.isCompleted shouldBe false
        }

        testF.futureValueUS
      }

      "have new transport be used for sends" in {
        // TODO(i26481): Enable new connection pool (test uses changeTransport())
        val env = RichEnvFactory.create(useNewConnectionPoolO = Some(false))
        val secondTransport = MockTransport(env.clock)

        val testF = for {
          _ <- env.changeTransport(secondTransport, None)
          _ <- env.sendAsync(Batch.empty(testedProtocolVersion)).value
        } yield {
          env.transport.lastSend.get() shouldBe None
          secondTransport.lastSend.get() should not be None

          env.transport.isClosing shouldBe true
          secondTransport.isClosing shouldBe false
        }

        testF.futureValueUS
        env.client.close()
      }

      "have new transport be used for logout" in {
        // TODO(i26481): Enable new connection pool (test uses changeTransport())
        val env = RichEnvFactory.create(useNewConnectionPoolO = Some(false))
        val secondTransport = MockTransport(env.clock)

        val testF = for {
          _ <- env.changeTransport(secondTransport, None)
          _ <- env.logout().value
        } yield {
          env.transport.logoutCalled shouldBe false
          secondTransport.logoutCalled shouldBe true
        }

        testF.futureValueUS
        env.client.close()
      }

      "have new transport be used for sends when there is subscription" in {
        // TODO(i26481): Enable new connection pool (test uses changeTransport())
        val env = RichEnvFactory.create(useNewConnectionPoolO = Some(false))
        val secondTransport = MockTransport(env.clock)

        val testF = for {
          _ <- env.subscribeAfter()
          _ <- env.changeTransport(secondTransport, None)
          _ <- env.sendAsync(Batch.empty(testedProtocolVersion)).value
        } yield {
          env.transport.lastSend.get() shouldBe None
          secondTransport.lastSend.get() should not be None
        }

        testF.futureValueUS
        env.client.close()
      }

      "have new transport be used with same sequencerId but different sequencer alias" in {
        // TODO(i26481): Enable new connection pool (test uses changeTransport())
        val env = RichEnvFactory.create(useNewConnectionPoolO = Some(false))
        val secondTransport = MockTransport(env.clock)

        val testF = for {
          _ <- env.subscribeAfter()
          _ <- env.changeTransport(
            SequencerTransports.single(
              SequencerAlias.tryCreate("somethingElse"),
              daSequencerId,
              secondTransport,
            ),
            None,
          )
          _ <- env.sendAsync(Batch.empty(testedProtocolVersion)).value
        } yield {
          env.transport.lastSend.get() shouldBe None
          secondTransport.lastSend.get() should not be None
        }

        testF.futureValueUS
        env.client.close()
      }

      "fail to reassign sequencerId" in {
        val env = RichEnvFactory.create()
        val secondTransport = MockTransport(env.clock)
        val secondSequencerId = SequencerId(
          UniqueIdentifier.tryCreate("da2", Namespace(Fingerprint.tryFromString("default")))
        )

        // When using the connection pool, this test does not make sense
        if (!env.useNewConnectionPool) {
          val testF = for {
            _ <- env.subscribeAfter()
            error <- loggerFactory
              .assertLogs(
                env
                  .changeTransport(
                    SequencerTransports.default(
                      secondSequencerId,
                      secondTransport,
                    ),
                    None,
                  ),
                _.errorMessage shouldBe "Adding or removing sequencer subscriptions is not supported at the moment",
              )
              .failed
          } yield {
            error
          }

          testF.futureValueUS shouldBe an[IllegalArgumentException]
          testF.futureValueUS.getMessage shouldBe "Adding or removing sequencer subscriptions is not supported at the moment"
        }
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
    def sendToHandler(event: SequencedSerializedEvent): FutureUnlessShutdown[Unit]

    def sendToHandler(event: SequencedEvent[ClosedEnvelope]): FutureUnlessShutdown[Unit] =
      sendToHandler(SequencedEventWithTraceContext(SequencerTestUtils.sign(event))(traceContext))
  }

  private case class OldStyleSubscriber[E](
      override val request: SubscriptionRequest,
      private val handler: SequencedEventHandler[E],
      override val subscription: MockSubscription[E],
  ) extends Subscriber[E] {
    override def sendToHandler(event: SequencedSerializedEvent): FutureUnlessShutdown[Unit] =
      handler(event).transform {
        case Success(UnlessShutdown.Outcome(Right(_))) => Success(UnlessShutdown.unit)
        case Success(UnlessShutdown.Outcome(Left(err))) =>
          subscription.closeSubscription(err)
          Success(UnlessShutdown.unit)
        case Failure(ex) =>
          subscription.closeSubscription(ex)
          Success(UnlessShutdown.unit)
        case Success(UnlessShutdown.AbortedDueToShutdown) =>
          Success(UnlessShutdown.unit)
      }
  }

  private case class SubscriberPekko[E](
      override val request: SubscriptionRequest,
      private val queue: BoundedSourceQueue[SequencedSerializedEvent],
      override val subscription: MockSubscription[E],
  ) extends Subscriber[E] {
    override def sendToHandler(event: SequencedSerializedEvent): FutureUnlessShutdown[Unit] =
      queue.offer(event) match {
        case QueueOfferResult.Enqueued =>
          // TODO(#13789) This may need more synchronization
          FutureUnlessShutdown.unit
        case QueueOfferResult.Failure(ex) =>
          logger.error(s"Failed to enqueue event", ex)
          fail("Failed to enqueue event")
        case other =>
          fail(s"Could not enqueue event $event: $other")
      }
  }

  private case class Env[+Client <: SequencerClient](
      client: Client,
      transport: MockTransport,
      pool: MockPool,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      sequencedEventStore: SequencedEventStore,
      timeTracker: SynchronizerTimeTracker,
      trafficStateController: TrafficStateController,
      clock: SimClock,
      useNewConnectionPool: Boolean,
  ) {
    def subscriber: Option[Subscriber[?]] =
      if (useNewConnectionPool) pool.connection.subscriber else transport.subscriber

    def subscribeAfter(
        priorTimestamp: CantonTimestamp = CantonTimestamp.MinValue,
        eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] = alwaysSuccessfulHandler,
    ): FutureUnlessShutdown[Unit] =
      client.subscribeAfter(
        priorTimestamp,
        None,
        eventHandler,
        timeTracker,
        PeriodicAcknowledgements.noAcknowledgements,
      )

    def changeTransport(
        newTransport: SequencerClientTransport & SequencerClientTransportPekko,
        newConnectionPoolConfigO: Option[SequencerConnectionXPoolConfig],
    )(implicit ev: Client <:< RichSequencerClient): FutureUnlessShutdown[Unit] =
      changeTransport(
        SequencerTransports.default(daSequencerId, newTransport),
        newConnectionPoolConfigO,
      )

    def changeTransport(
        sequencerTransports: SequencerTransports[?],
        newConnectionPoolConfigO: Option[SequencerConnectionXPoolConfig],
    )(implicit
        ev: Client <:< RichSequencerClient
    ): FutureUnlessShutdown[Unit] =
      ev(client)
        .changeTransport(sequencerTransports, newConnectionPoolConfigO)
        .valueOrFail("changeTransport")

    def sendAsync(
        batch: Batch[DefaultOpenEnvelope],
        messageId: MessageId = client.generateMessageId,
        amplify: Boolean = false,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SendAsyncClientError, Unit] = {
      implicit val metricsContext: MetricsContext = MetricsContext.Empty
      client.send(batch, messageId = messageId, amplify = amplify)
    }

    def logout(): EitherT[FutureUnlessShutdown, Status, Unit] = client.logout()
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

  /** A sendAsync resonse: either a sync error, or an action before returning Unit.
    */
  type SendAsyncResponse = Either[SendAsyncClientResponseError, SimClock => Unit]

  private class MockTransport(
      clock: SimClock,
      firstSendAsyncResponseO: Option[SendAsyncResponse],
  ) extends SequencerClientTransport
      with SequencerClientTransportPekko
      with NamedLogging {

    private val logoutCalledRef = new AtomicReference[Boolean](false)
    val acknowledgedTimestamps: AtomicReference[Set[CantonTimestamp]] = new AtomicReference(
      Set.empty
    )

    private val sendAsyncResponseRef =
      new AtomicReference[Option[SendAsyncResponse]](firstSendAsyncResponseO)

    override def getTime(timeout: Duration)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[CantonTimestamp]] =
      EitherT.rightT(None)

    override def logout()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, Status, Unit] = {
      logoutCalledRef.set(true)
      EitherT.pure(())
    }

    def logoutCalled: Boolean = logoutCalledRef.get()

    override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing

    private val subscriberRef = new AtomicReference[Option[Subscriber[?]]](None)

    // When using a parallel execution context, the order of asynchronous operations within the SequencerClient
    // is not deterministic which can delay the subscription. This is why we add some retry policy to avoid flaky tests
    def subscriber: Option[Subscriber[?]] = {
      @tailrec def subscriber(retry: Int): Option[Subscriber[?]] =
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

    val lastSend = new AtomicReference[Option[SentSubmission]](None)
    def lastSubmissionTime: Option[CantonTimestamp] = lastSend.get.map(_.submissionTime)

    override def acknowledgeSigned(request: SignedContent[AcknowledgeRequest])(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Boolean] = {
      acknowledgedTimestamps.getAndUpdate(_ + request.content.timestamp)

      EitherT.rightT(true)
    }

    override def getTrafficStateForMember(request: GetTrafficStateForMemberRequest)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, GetTrafficStateForMemberResponse] =
      EitherT.pure(
        GetTrafficStateForMemberResponse(
          Some(
            TrafficState(
              extraTrafficPurchased = NonNegativeLong.zero,
              // Use the timestamp as the traffic consumed
              // This allows us to assert how the state returned by this function is used by the state controller to
              // refresh its own traffic state
              extraTrafficConsumed =
                NonNegativeLong.tryCreate(Math.abs(request.timestamp.toProtoPrimitive)),
              baseTrafficRemainder = NonNegativeLong.zero,
              lastConsumedCost = NonNegativeLong.zero,
              timestamp = request.timestamp,
              serial = None,
            )
          ),
          testedProtocolVersion,
        )
      )

    private def sendAsync(
        request: SubmissionRequest
    ): EitherT[Future, SendAsyncClientResponseError, Unit] = {
      lastSend.set(Some(SentSubmission(clock.now, request)))

      val responseE = sendAsyncResponseRef.getAndSet(None) match {
        case None => Right(())
        case Some(Left(error)) => Left(error)
        case Some(Right(action)) => Right(action.apply(clock))
      }
      responseE.toEitherT
    }

    override def sendAsyncSigned(
        request: SignedContent[SubmissionRequest],
        timeout: Duration,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SendAsyncClientResponseError, Unit] =
      sendAsync(request.content).mapK(FutureUnlessShutdown.outcomeK)

    override def subscribe[E](request: SubscriptionRequest, handler: SequencedEventHandler[E])(
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

    override protected def loggerFactory: NamedLoggerFactory =
      SequencerClientTest.this.loggerFactory

    override def downloadTopologyStateForInit(request: TopologyStateForInitRequest)(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, TopologyStateForInitResponse] = ???

    override type SubscriptionError = Uninhabited

    override def subscribe(request: SubscriptionRequest)(implicit
        traceContext: TraceContext
    ): SequencerSubscriptionPekko[SubscriptionError] = {
      // Choose a sufficiently large queue size so that we can test throttling
      val (queue, sourceQueue) =
        Source.queue[SequencedSerializedEvent](200).preMaterialize()(materializer)

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

    override def subscriptionRetryPolicyPekko
        : SubscriptionErrorRetryPolicyPekko[SubscriptionError] =
      SubscriptionErrorRetryPolicyPekko.never

    override def downloadTopologyStateForInitHash(request: TopologyStateForInitRequest)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitHashResponse] = ???
  }

  private object MockTransport {
    def apply(
        clock: SimClock,
        firstSendAsyncResponseO: Option[SendAsyncResponse] = None,
    ): MockTransport & SequencerClientTransportPekko.Aux[Uninhabited] =
      new MockTransport(clock, firstSendAsyncResponseO)
  }

  private class MockConnection(
      override val name: String,
      ack: CantonTimestamp => Unit,
      clock: SimClock,
      firstSendAsyncResponseO: Option[SendAsyncResponse],
  ) extends SequencerConnectionX {
    override val health: SequencerConnectionXHealth =
      new SequencerConnectionXHealth.AlwaysValidated(s"$name-health", logger)

    private val sendAsyncResponseRef =
      new AtomicReference[Option[SendAsyncResponse]](firstSendAsyncResponseO)

    override def config: ConnectionXConfig = ConnectionXConfig(
      name = name,
      endpoint = Endpoint("dummy-endpoint", Port.tryCreate(0)),
      transportSecurity = false,
      customTrustCertificates = None,
      expectedSequencerIdO = None,
      tracePropagation = TracingConfig.Propagation.Disabled,
    )

    override def attributes: ConnectionAttributes =
      ConnectionAttributes(
        DefaultTestIdentities.physicalSynchronizerId,
        DefaultTestIdentities.daSequencerId,
        defaultStaticSynchronizerParameters,
      )

    override def fail(reason: String)(implicit traceContext: TraceContext): Unit = ???

    override def fatal(reason: String)(implicit traceContext: TraceContext): Unit = ()

    val lastSend = new AtomicReference[Option[SentSubmission]](None)
    def lastSubmissionTime: Option[CantonTimestamp] = lastSend.get.map(_.submissionTime)

    override def sendAsync(request: SignedContent[SubmissionRequest], timeout: Duration)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SendAsyncClientResponseError, Unit] = {
      lastSend.set(Some(SentSubmission(clock.now, request.content)))

      val responseE = sendAsyncResponseRef.getAndSet(None) match {
        case None => Right(())
        case Some(Left(error)) => Left(error)
        case Some(Right(action)) => Right(action.apply(clock))
      }
      responseE.toEitherT
    }

    override def acknowledgeSigned(
        signedRequest: SignedContent[AcknowledgeRequest],
        timeout: Duration,
    )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Boolean] = {
      ack(signedRequest.content.timestamp)

      EitherT.rightT(true)
    }

    override def getTrafficStateForMember(
        request: GetTrafficStateForMemberRequest,
        timeout: Duration,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, GetTrafficStateForMemberResponse] = ???

    override def logout()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, Status, Unit] = ???

    override def getTime(timeout: Duration)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[CantonTimestamp]] = ???

    override def downloadTopologyStateForInit(
        request: TopologyStateForInitRequest,
        timeout: Duration,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitResponse] = ???

    private val subscriberRef = new AtomicReference[Option[Subscriber[?]]](None)

    // When using a parallel execution context, the order of asynchronous operations within the SequencerClient
    // is not deterministic which can delay the subscription. This is why we add some retry policy to avoid flaky tests
    def subscriber: Option[Subscriber[?]] = {
      @tailrec def subscriber(retry: Int): Option[Subscriber[?]] =
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

    override def subscribe[E](
        request: SubscriptionRequest,
        handler: SequencedEventHandler[E],
        timeout: Duration,
    )(implicit traceContext: TraceContext): Either[String, SequencerSubscription[E]] = {
      val subscription = new MockSubscription[E]

      if (
        !subscriberRef.compareAndSet(None, Some(OldStyleSubscriber(request, handler, subscription)))
      ) {
        fail("subscribe has already been called by this client")
      }

      Right(subscription)
    }

    override def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
      SubscriptionErrorRetryPolicy.never

    override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing

    override protected def loggerFactory: NamedLoggerFactory =
      SequencerClientTest.this.loggerFactory

    override def downloadTopologyStateForInitHash(
        request: TopologyStateForInitRequest,
        timeout: Duration,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitHashResponse] = ???
  }

  private class MockPool(clock: SimClock, firstSendAsyncResponseO: Option[SendAsyncResponse])
      extends SequencerConnectionXPool {
    val acknowledgedTimestamps: AtomicReference[Set[CantonTimestamp]] = new AtomicReference(
      Set.empty
    )

    val connection =
      new MockConnection(
        name = "test",
        ack = ts => acknowledgedTimestamps.getAndUpdate(_ + ts),
        clock = clock,
        firstSendAsyncResponseO = firstSendAsyncResponseO,
      )

    override protected def loggerFactory: NamedLoggerFactory =
      SequencerClientTest.this.loggerFactory

    override def physicalSynchronizerIdO: Option[PhysicalSynchronizerId] = ???

    override def staticSynchronizerParametersO: Option[StaticSynchronizerParameters] = ???

    override def start()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SequencerConnectionXPoolError, Unit] = ???

    override def config: SequencerConnectionXPool.SequencerConnectionXPoolConfig =
      SequencerConnectionXPoolConfig(
        connections = NonEmpty(Seq, connection.config),
        trustThreshold = PositiveInt.one,
        minRestartConnectionDelay = cantonConfig.NonNegativeFiniteDuration.Zero,
        maxRestartConnectionDelay = cantonConfig.NonNegativeFiniteDuration.Zero,
        warnConnectionValidationDelay = cantonConfig.NonNegativeFiniteDuration.Zero,
      )

    override def updateConfig(newConfig: SequencerConnectionXPool.SequencerConnectionXPoolConfig)(
        implicit traceContext: TraceContext
    ): Either[SequencerConnectionXPool.SequencerConnectionXPoolError, Unit] = ???

    override def health: SequencerConnectionXPool.SequencerConnectionXPoolHealth = ???

    override def nbSequencers: NonNegativeInt = ???

    override def nbConnections: NonNegativeInt = ???

    override def getConnections(requester: String, nb: PositiveInt, exclusions: Set[SequencerId])(
        implicit traceContext: TraceContext
    ): Set[SequencerConnectionX] = Set(connection)

    override def getOneConnectionPerSequencer(requester: String)(implicit
        traceContext: TraceContext
    ): Map[SequencerId, SequencerConnectionX] = ???

    override def getAllConnections()(implicit
        traceContext: TraceContext
    ): Seq[SequencerConnectionX] = ???

    override def contents: Map[SequencerId, Set[SequencerConnectionX]] = ???

    override protected val timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing

    override def isThresholdStillReachable(
        threshold: PositiveInt,
        ignored: Set[ConnectionXConfig],
        extraUndecided: NonNegativeInt,
    )(implicit traceContext: TraceContext): Boolean = true
  }

  private object MockPool {
    def apply(
        clock: SimClock,
        firstSendAsyncResponseO: Option[SendAsyncResponse] = None,
    ): MockPool =
      new MockPool(clock, firstSendAsyncResponseO)
  }

  private val overloadedError = SendAsyncClientError.RequestRefused(
    SendAsyncError.SendAsyncErrorGrpc(
      GrpcError.GrpcRequestRefusedByServer(
        request = "test-request",
        serverName = "test-server",
        status = Status.UNAVAILABLE,
        optTrailers = None,
        decodedCantonError = Some(
          DecodedCantonError(
            code = SequencerErrors.Overloaded,
            cause = "test-cause",
            correlationId = None,
            traceId = None,
            context = Map.empty,
            resources = Seq.empty,
          )
        ),
      )
    )
  )

  private val senderUnknownError = SendAsyncClientError.RequestRefused(
    SendAsyncError.SendAsyncErrorGrpc(
      GrpcError.GrpcRequestRefusedByServer(
        request = "test-request",
        serverName = "test-server",
        status = Status.UNAVAILABLE,
        optTrailers = None,
        decodedCantonError = Some(
          DecodedCantonError(
            code = SequencerErrors.SenderUnknown,
            cause = "test-cause",
            correlationId = None,
            traceId = None,
            context = Map.empty,
            resources = Seq.empty,
          )
        ),
      )
    )
  )

  private implicit class EnrichedSequencerClient(client: RichSequencerClient) {
    // flush needs to be called twice in order to finish asynchronous processing
    // (see comment around shutdown in SequencerClient). So we have this small
    // helper for the tests.
    def flushClean(): FutureUnlessShutdown[Unit] = for {
      _ <- client.flush()
      _ <- client.flush()
    } yield ()
  }

  private val eventAlwaysValid: SequencedEventValidator = SequencedEventValidator.noValidation(
    DefaultTestIdentities.physicalSynchronizerId,
    warn = false,
  )

  private trait EnvFactory[+Client <: SequencerClient] {
    def create(
        psid: PhysicalSynchronizerId = DefaultTestIdentities.physicalSynchronizerId,
        storedEvents: Seq[SequencedEvent[ClosedEnvelope]] = Seq.empty,
        cleanPrehead: Option[SequencerCounterCursorPrehead] = None,
        eventValidator: SequencedEventValidator = eventAlwaysValid,
        options: SequencerClientConfig = SequencerClientConfig(),
        topologyO: Option[SynchronizerCryptoClient] = None,
        initializeCounterAllocatorTo: Option[SequencerCounter] = None,
        mockExitOnFatalErrorO: Option[(String, TracedLogger) => TraceContext => Unit] = None,
        synchronizerPredecessor: Option[SynchronizerPredecessor] = None,
        useNewConnectionPoolO: Option[Boolean] = None,
        amplificationConfig: SubmissionRequestAmplification =
          SubmissionRequestAmplification.NoAmplification,
        firstSendAsyncResponseO: Option[SendAsyncResponse] = None,
    )(implicit closeContext: CloseContext): Env[Client]

    protected def preloadStores(
        storedEvents: Seq[SequencedEvent[ClosedEnvelope]],
        cleanPrehead: Option[SequencerCounterCursorPrehead],
        sequencedEventStore: SequencedEventStore,
        sequencerCounterTrackerStore: SequencerCounterTrackerStore,
        initializeCounterAllocatorTo: Option[SequencerCounter],
    ): Unit = {
      val signedEvents = storedEvents.map(SequencerTestUtils.sign)
      val preloadStores = for {
        _ <- initializeCounterAllocatorTo.traverse_(counter =>
          sequencedEventStore.reinitializeFromDbOrSetLowerBound(counter)
        )
        _ <- sequencedEventStore.store(
          signedEvents.map(SequencedEventWithTraceContext(_)(TraceContext.empty))
        )
        _ <- cleanPrehead.traverse_(prehead =>
          sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(prehead)
        )
      } yield ()
      preloadStores.futureValueUS
    }

    protected def maxRequestSizeLookup: DynamicSynchronizerParametersLookup[
      SynchronizerParametersLookup.SequencerSynchronizerParameters
    ] = {
      val topologyClient = mock[SynchronizerTopologyClient]
      val mockTopologySnapshot = mock[TopologySnapshot]
      when(topologyClient.currentSnapshotApproximation(any[TraceContext]))
        .thenReturn(mockTopologySnapshot)
      when(
        mockTopologySnapshot.findDynamicSynchronizerParametersOrDefault(
          any[ProtocolVersion],
          anyBoolean,
        )(any[TraceContext])
      )
        .thenReturn(FutureUnlessShutdown.pure(TestSynchronizerParameters.defaultDynamic))
      SynchronizerParametersLookup.forSequencerSynchronizerParameters(
        None,
        topologyClient,
        loggerFactory,
      )
    }

  }

  private object MockRequestSigner extends RequestSigner {
    override def signRequest[A <: HasCryptographicEvidence](
        request: A,
        hashPurpose: HashPurpose,
        snapshot: Option[SyncCryptoApi],
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, SignedContent[A]] = {
      val signedContent = SignedContent(
        request,
        SymbolicCrypto.emptySignature,
        None,
        testedProtocolVersion,
      )
      EitherT(FutureUnlessShutdown.pure(Either.right[String, SignedContent[A]](signedContent)))
    }
  }

  private class ConstantSequencedEventValidatorFactory(eventValidator: SequencedEventValidator)
      extends SequencedEventValidatorFactory {
    override def create(loggerFactory: NamedLoggerFactory)(implicit
        traceContext: TraceContext
    ): SequencedEventValidator =
      eventValidator
  }

  private object TestProtocolMessage
  private class TestProtocolMessage()
      extends UnsignedProtocolMessage
      with IgnoreInSerializationTestExhaustivenessCheck {
    override def psid: PhysicalSynchronizerId = fail("shouldn't be used")

    override def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type] =
      fail("shouldn't be used")

    override protected val companionObj: AnyRef = TestProtocolMessage

    override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
      v30.EnvelopeContent.SomeEnvelopeContent.Empty

    override def productElement(n: Int): Any = fail("shouldn't be used")
    override def productArity: Int = fail("shouldn't be used")
    override def canEqual(that: Any): Boolean = fail("shouldn't be used")
  }

  private object RichEnvFactory extends EnvFactory[RichSequencerClient] {
    override def create(
        psid: PhysicalSynchronizerId = DefaultTestIdentities.physicalSynchronizerId,
        storedEvents: Seq[SequencedEvent[ClosedEnvelope]],
        cleanPrehead: Option[SequencerCounterCursorPrehead],
        eventValidator: SequencedEventValidator,
        options: SequencerClientConfig,
        topologyO: Option[SynchronizerCryptoClient],
        initializeCounterAllocatorTo: Option[SequencerCounter],
        mockExitOnFatalErrorO: Option[(String, TracedLogger) => TraceContext => Unit],
        synchronizerPredecessor: Option[SynchronizerPredecessor],
        useNewConnectionPoolO: Option[Boolean],
        amplificationConfig: SubmissionRequestAmplification,
        firstSendAsyncResponseO: Option[SendAsyncResponse],
    )(implicit closeContext: CloseContext): Env[RichSequencerClient] = {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val timeouts = DefaultProcessingTimeouts.testing
      val transport = MockTransport(clock, firstSendAsyncResponseO)
      val sendTrackerStore = new InMemorySendTrackerStore()
      val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory, timeouts)
      val sequencerCounterTrackerStore =
        new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
      val timeTracker = new SynchronizerTimeTracker(
        SynchronizerTimeTrackerConfig(),
        clock,
        new MockTimeRequestSubmitter(),
        timeouts,
        loggerFactory,
      )
      val eventValidatorFactory = new ConstantSequencedEventValidatorFactory(eventValidator)

      val topologyClient =
        topologyO.getOrElse(
          TestingTopology(Set(DefaultTestIdentities.physicalSynchronizerId))
            .build(loggerFactory)
            .forOwnerAndSynchronizer(mediatorId)
        )
      val trafficStateController = new TrafficStateController(
        mediatorId,
        loggerFactory,
        topologyClient,
        TrafficState.empty(CantonTimestamp.MinValue),
        testedProtocolVersion,
        new EventCostCalculator(loggerFactory),
        TrafficConsumptionMetrics.noop,
        DefaultTestIdentities.physicalSynchronizerId,
      )
      val sendTracker =
        new SendTracker(
          Map.empty,
          sendTrackerStore,
          metrics,
          loggerFactory,
          timeouts,
          Some(trafficStateController),
        )

      val connectionPool = MockPool(clock, firstSendAsyncResponseO)

      // TODO(i26481): adjust when everything in this test can be enabled for the connection pool
      val useNewConnectionPool = useNewConnectionPoolO.getOrElse(true)

      val transports = SequencerTransports
        .from(
          sequencerTransportsMapO =
            Option.when(!useNewConnectionPool)(NonEmpty(Map, SequencerAlias.Default -> transport)),
          expectedSequencersO = Option.when(!useNewConnectionPool)(
            NonEmpty(Map, SequencerAlias.Default -> DefaultTestIdentities.daSequencerId)
          ),
          sequencerSignatureThreshold = PositiveInt.one,
          sequencerLivenessMargin = NonNegativeInt.zero,
          submissionRequestAmplification = amplificationConfig,
          sequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
        )
        .value

      val client = new RichSequencerClientImpl(
        psid,
        synchronizerPredecessor = synchronizerPredecessor,
        mediatorId,
        transports,
        connectionPool = connectionPool,
        options.copy(useNewConnectionPool = useNewConnectionPool),
        TestingConfigInternal(),
        maxRequestSizeLookup,
        timeouts,
        eventValidatorFactory,
        clock,
        MockRequestSigner,
        sequencedEventStore,
        sendTracker,
        CommonMockMetrics.sequencerClient,
        None,
        replayEnabled = false,
        topologyClient,
        LoggingConfig(),
        Some(trafficStateController),
        exitOnFatalErrors = mockExitOnFatalErrorO.nonEmpty, // only "exit" when exit mock specified
        loggerFactory,
        futureSupervisor,
      )(parallelExecutionContext, tracer) {
        override protected def exitOnFatalError(
            message: String,
            logger: TracedLogger,
        )(implicit traceContext: TraceContext): Unit =
          mockExitOnFatalErrorO match {
            case None => super.exitOnFatalError(message, logger)(traceContext)
            case Some(exitOnFatalError) => exitOnFatalError(message, logger)(traceContext)
          }
      }

      preloadStores(
        storedEvents,
        cleanPrehead,
        sequencedEventStore,
        sequencerCounterTrackerStore,
        initializeCounterAllocatorTo,
      )

      Env(
        client,
        transport,
        connectionPool,
        sequencerCounterTrackerStore,
        sequencedEventStore,
        timeTracker,
        trafficStateController,
        clock,
        useNewConnectionPool,
      )
    }
  }

  private object PekkoEnvFactory extends EnvFactory[SequencerClient] {
    override def create(
        psid: PhysicalSynchronizerId,
        storedEvents: Seq[SequencedEvent[ClosedEnvelope]],
        cleanPrehead: Option[SequencerCounterCursorPrehead],
        eventValidator: SequencedEventValidator,
        options: SequencerClientConfig,
        topologyO: Option[SynchronizerCryptoClient],
        initializeCounterAllocatorTo: Option[SequencerCounter],
        mockExitOnFatalErrorO: Option[(String, TracedLogger) => TraceContext => Unit],
        synchronizerPredecessor: Option[SynchronizerPredecessor],
        useNewConnectionPoolO: Option[Boolean],
        amplificationConfig: SubmissionRequestAmplification,
        firstSendAsyncResponseO: Option[SendAsyncResponse],
    )(implicit closeContext: CloseContext): Env[SequencerClient] = {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val timeouts = DefaultProcessingTimeouts.testing
      val transport = MockTransport(clock)
      val sendTrackerStore = new InMemorySendTrackerStore()
      val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory, timeouts)
      val sequencerCounterTrackerStore =
        new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
      val timeTracker = new SynchronizerTimeTracker(
        SynchronizerTimeTrackerConfig(),
        clock,
        new MockTimeRequestSubmitter(),
        timeouts,
        loggerFactory,
      )
      val eventValidatorFactory = new ConstantSequencedEventValidatorFactory(eventValidator)
      val topologyClient = topologyO.getOrElse(
        TestingTopology()
          .build(loggerFactory)
          .forOwnerAndSynchronizer(participant1, DefaultTestIdentities.physicalSynchronizerId)
      )
      val trafficStateController = new TrafficStateController(
        participant1,
        loggerFactory,
        topologyClient,
        TrafficState.empty(CantonTimestamp.MinValue),
        testedProtocolVersion,
        new EventCostCalculator(loggerFactory),
        TrafficConsumptionMetrics.noop,
        DefaultTestIdentities.physicalSynchronizerId,
      )
      val sendTracker =
        new SendTracker(
          Map.empty,
          sendTrackerStore,
          metrics,
          loggerFactory,
          timeouts,
          Some(trafficStateController),
        )

      val connectionPool = MockPool(clock)

      // TODO(i26481): adjust when the new connection pool is stable
      // The subscription pool does not support the Pekko sequencer client
      val useNewConnectionPool = useNewConnectionPoolO.getOrElse(false)
      val client = new SequencerClientImplPekko(
        DefaultTestIdentities.physicalSynchronizerId,
        participant1,
        SequencerTransports.default(DefaultTestIdentities.daSequencerId, transport),
        connectionPool = connectionPool,
        options.copy(useNewConnectionPool = useNewConnectionPool),
        TestingConfigInternal(),
        maxRequestSizeLookup,
        timeouts,
        eventValidatorFactory,
        clock,
        MockRequestSigner,
        sequencedEventStore,
        sendTracker,
        CommonMockMetrics.sequencerClient,
        None,
        replayEnabled = false,
        topologyClient,
        LoggingConfig(),
        Some(trafficStateController),
        exitOnTimeout = false,
        loggerFactory,
        futureSupervisor,
      )(PrettyInstances.prettyUninhabited, parallelExecutionContext, tracer, materializer)

      preloadStores(
        storedEvents,
        cleanPrehead,
        sequencedEventStore,
        sequencerCounterTrackerStore,
        initializeCounterAllocatorTo,
      )

      Env(
        client,
        transport,
        connectionPool,
        sequencerCounterTrackerStore,
        sequencedEventStore,
        timeTracker,
        trafficStateController,
        clock,
        useNewConnectionPool,
      )
    }
  }
}

object SequencerClientTest {
  private final case class SentSubmission(
      submissionTime: CantonTimestamp,
      request: SubmissionRequest,
  )
}
