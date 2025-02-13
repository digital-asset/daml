// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import cats.syntax.functor.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.PayloadToEventTimeBoundExceeded
import com.digitalasset.canton.synchronizer.sequencer.store.{
  BytesPayload,
  CounterCheckpoint,
  DeliverErrorStoreEvent,
  DeliverStoreEvent,
  InMemorySequencerStore,
  PayloadId,
  Presequenced,
  SavePayloadsError,
  Sequenced,
  SequencerMemberId,
  SequencerWriterStore,
  StoreEvent,
}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.topology.{Member, ParticipantId, SequencerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutorService,
  ProtocolVersionChecksAsyncWordSpec,
  SequencerCounter,
  config,
}
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.QueueOfferResult
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.Assertion
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
class SequencerWriterSourceTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutorService
    with ProtocolVersionChecksAsyncWordSpec
    with FailOnShutdown {

  class MockEventSignaller extends EventSignaller {
    private val listenerRef =
      new AtomicReference[Option[WriteNotification => Unit]](None)

    def attachWriteListener(listener: WriteNotification => Unit): AutoCloseable = {
      if (!listenerRef.compareAndSet(None, Some(listener))) {
        // suggests something is screwed up with the test
        fail("There is already an active listener subscribed")
      }

      () => listenerRef.set(None)
    }

    override def notifyOfLocalWrite(
        notification: WriteNotification
    )(implicit traceContext: TraceContext): Future[Unit] =
      Future.successful {
        listenerRef.get().foreach(listener => listener(notification))
      }

    override def readSignalsForMember(
        member: Member,
        memberId: SequencerMemberId,
    )(implicit traceContext: TraceContext): Source[ReadSignal, NotUsed] =
      fail("shouldn't be used")

    override def close(): Unit = ()
  }

  private class Env(keepAliveInterval: Option[NonNegativeFiniteDuration])
      extends FlagCloseableAsync {
    override val timeouts: ProcessingTimeout = SequencerWriterSourceTest.this.timeouts
    protected val logger: TracedLogger = SequencerWriterSourceTest.this.logger
    implicit val actorSystem: ActorSystem = ActorSystem()
    val instanceIndex: Int = 0
    val testWriterConfig: SequencerWriterConfig =
      SequencerWriterConfig
        .LowLatency(
          checkpointInterval = NonNegativeFiniteDuration.tryOfSeconds(1).toConfig
        )
    lazy val sequencerMember: Member = SequencerId(
      UniqueIdentifier.tryFromProtoPrimitive("sequencer::namespace")
    )
    class InMemoryStoreWithTimeAdvancement(lFactory: NamedLoggerFactory)(implicit
        ec: ExecutionContext
    ) extends InMemorySequencerStore(
          testedProtocolVersion,
          sequencerMember,
          blockSequencerMode = true,
          lFactory,
        )(
          ec
        ) {
      private val timeAdvancement = new AtomicReference[java.time.Duration](java.time.Duration.ZERO)

      def setClockAdvanceBeforeSavePayloads(duration: java.time.Duration): Unit =
        timeAdvancement.set(duration)

      override def savePayloads(
          payloadsToInsert: NonEmpty[Seq[BytesPayload]],
          instanceDiscriminator: UUID,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, SavePayloadsError, Unit] = {
        clock.advance(timeAdvancement.get())
        super.savePayloads(payloadsToInsert, instanceDiscriminator)
      }
    }

    val store =
      new InMemoryStoreWithTimeAdvancement(loggerFactory) // allows setting time advancements
    private val writerStore = SequencerWriterStore.singleInstance(store)
    val clock = new SimClock(loggerFactory = loggerFactory)

    val eventSignaller = new MockEventSignaller

    // explicitly pass a real execution context so shutdowns don't deadlock while Await'ing completion of the done
    // future while still finishing up running tasks that require an execution context
    val (writer, doneF) = PekkoUtil.runSupervised(
      SequencerWriterSource(
        testWriterConfig,
        totalNodeCount = PositiveInt.tryCreate(1),
        keepAliveInterval,
        writerStore,
        clock,
        eventSignaller,
        loggerFactory,
        testedProtocolVersion,
        SequencerMetrics.noop(suiteName),
        timeouts,
        blockSequencerMode = true,
      )(executorService, implicitly[TraceContext], implicitly[ErrorLoggingContext])
        .toMat(Sink.ignore)(Keep.both),
      errorLogMessagePrefix = "Writer flow failed",
    )

    def completeFlow(): Future[Unit] = {
      writer.complete()
      doneF.void
    }

    def offerDeliverOrFail(event: Presequenced[DeliverStoreEvent[BytesPayload]]): Unit =
      writer.deliverEventQueue.offer(event) shouldBe QueueOfferResult.Enqueued

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
      Seq(
        SyncCloseable("sequencer", writer.complete()),
        AsyncCloseable("done", doneF, config.NonNegativeFiniteDuration(10.seconds)),
        AsyncCloseable(
          "actorSystem",
          actorSystem.terminate(),
          config.NonNegativeFiniteDuration(10.seconds),
        ),
      )
  }

  private def withEnv(
      keepAliveInterval: Option[NonNegativeFiniteDuration] = None
  )(testCode: Env => Future[Assertion]): Future[Assertion] = {
    val env = new Env(keepAliveInterval)
    val result = testCode(env)
    result.onComplete(_ => env.close())
    result
  }

  private def getErrorMessage(errorO: Option[ByteString]): String =
    DeliverErrorStoreEvent.fromByteString(errorO, testedProtocolVersion).toString

  private val alice = ParticipantId("alice")
  private val bob = ParticipantId("bob")
  private val charlie = ParticipantId("charlie")
  private val messageId1 = MessageId.tryCreate("1")
  private val messageId2 = MessageId.tryCreate("2")
  private val nextPayload = new AtomicLong(1)
  def generatePayload(): BytesPayload = {
    val n = nextPayload.getAndIncrement()
    BytesPayload(PayloadId(CantonTimestamp.assertFromLong(n)), ByteString.copyFromUtf8(n.toString))
  }
  private val payload1 = generatePayload()
  private val payload2 = generatePayload()

  "payload to event time bound" should {

    // this test doesn't work with block sequencers
    "prevent sequencing deliver events if their payloads are too old" in withEnv() { env =>
      import env.*

      val nowish = clock.now.plusSeconds(10)
      clock.advanceTo(nowish)

      for {
        aliceId <- store.registerMember(alice, CantonTimestamp.Epoch)
        deliver1 = DeliverStoreEvent.ensureSenderReceivesEvent(
          aliceId,
          messageId1,
          Set.empty,
          payload1,
          None,
          None,
        )
        _ <- loggerFactory.assertLogs(
          {
            offerDeliverOrFail(
              Presequenced.alwaysValid(
                deliver1,
                blockSequencerTimestamp = Some(
                  nowish.plus(
                    testWriterConfig.payloadToEventMargin.asJava.plusSeconds(1)
                  )
                ),
              )
            )
            FutureUnlessShutdown.outcomeF(completeFlow())
          },
          _.shouldBeCantonErrorCode(PayloadToEventTimeBoundExceeded),
        )
        events <- store.readEvents(aliceId, alice)
      } yield events.events shouldBe empty
    }
  }

  "max sequencing time" should {
    "drop sends above the max sequencing time" in withEnv() { env =>
      import env.*
      val nowish = clock.now.plusSeconds(10)
      clock.advanceTo(nowish)

      val beforeNow = nowish.minusSeconds(5)
      val longAfterNow = nowish.plusSeconds(5)

      for {
        aliceId <- store.registerMember(alice, CantonTimestamp.Epoch)
        deliver1 = DeliverStoreEvent.ensureSenderReceivesEvent(
          aliceId,
          messageId1,
          Set.empty,
          payload1,
          None,
          None,
        )
        deliver2 = DeliverStoreEvent.ensureSenderReceivesEvent(
          aliceId,
          messageId2,
          Set.empty,
          payload2,
          None,
          None,
        )
        _ <- {
          offerDeliverOrFail(
            Presequenced.withMaxSequencingTime(
              deliver1,
              beforeNow,
              Some(nowish),
            )
          )
          offerDeliverOrFail(
            Presequenced.withMaxSequencingTime(
              deliver2,
              longAfterNow,
              Some(nowish),
            )
          )
          FutureUnlessShutdown.outcomeF(completeFlow())
        }

        events <- store.readEvents(aliceId, alice)
      } yield {
        events.events should have size 1
        events.events.headOption.map(_.event).value should matchPattern {
          case DeliverStoreEvent(_, `messageId2`, _, _, _, _, _) =>
        }
      }
    }
  }

  "topology timestamp tolerance" should {
    def sendWithTopologyTimestamp(
        nowish: CantonTimestamp,
        validTopologyTimestamp: CantonTimestamp,
        invalidTopologyTimestamp: CantonTimestamp,
    )(implicit
        env: Env
    ): Future[Seq[StoreEvent[?]]] = {
      import env.*

      clock.advanceTo(nowish)

      for {
        aliceId <- store.registerMember(alice, CantonTimestamp.Epoch).failOnShutdown
        deliver1 = DeliverStoreEvent.ensureSenderReceivesEvent(
          aliceId,
          messageId1,
          Set.empty,
          payload1,
          Some(validTopologyTimestamp),
          None,
        )
        deliver2 = DeliverStoreEvent.ensureSenderReceivesEvent(
          aliceId,
          messageId2,
          Set.empty,
          payload1,
          Some(invalidTopologyTimestamp),
          None,
        )
        _ = offerDeliverOrFail(
          Presequenced.alwaysValid(deliver1, Some(nowish))
        )
        _ = offerDeliverOrFail(
          Presequenced.alwaysValid(
            deliver2,
            Some(nowish.immediateSuccessor),
          )
        )
        _ <- completeFlow()
        events <- store.readEvents(aliceId, alice).failOnShutdown
      } yield {
        events.events should have size 2
        events.events.map(_.event)
      }
    }

    /*
      Since ordering of the events is not guaranteed, we sort them to ease
      the test.
     */
    def sortByMessageId[P](events: Seq[StoreEvent[P]]): Seq[StoreEvent[P]] =
      events.sortBy(_.messageId.unwrap)

    "cause errors if way ahead of valid signing window" in withEnv() { implicit env =>
      val nowish = CantonTimestamp.Epoch.plusSeconds(10)

      // upper bound is inclusive
      val margin = NonNegativeFiniteDuration.tryOfMillis(1)
      val validTopologyTimestamp = nowish

      for {
        events <- sendWithTopologyTimestamp(
          nowish,
          validTopologyTimestamp = validTopologyTimestamp,
          invalidTopologyTimestamp = validTopologyTimestamp + margin,
        )
        sortedEvents = sortByMessageId(events)
      } yield {
        inside(sortedEvents.head) { case event: DeliverStoreEvent[_] =>
          event.messageId shouldBe messageId1
        }

        inside(sortedEvents(1)) { case DeliverErrorStoreEvent(_, _, errorO, _, _) =>
          getErrorMessage(errorO) should (include("Invalid topology timestamp")
            and include("The topology timestamp must be before or at "))
        }
      }
    }
  }

  "deliver with unknown recipients" should {
    "instead write an error" in withEnv() { implicit env =>
      import env.*

      for {
        aliceId <- store.registerMember(alice, CantonTimestamp.Epoch)

        batch = Batch.fromClosed(
          testedProtocolVersion,
          ClosedEnvelope.create(
            ByteString.EMPTY,
            Recipients.cc(bob),
            Seq.empty,
            testedProtocolVersion,
          ),
        )
        _ <- valueOrFail(
          writer
            .blockSequencerWrite(
              SubmissionOutcome.Deliver(
                SubmissionRequest.tryCreate(
                  alice,
                  MessageId.tryCreate("test-unknown-recipients"),
                  batch = batch,
                  maxSequencingTime = CantonTimestamp.MaxValue,
                  topologyTimestamp = None,
                  aggregationRule = None,
                  submissionCost = None,
                  protocolVersion = testedProtocolVersion,
                ),
                sequencingTime = CantonTimestamp.Epoch.immediateSuccessor,
                deliverToMembers = Set(alice, bob),
                batch = batch,
                submissionTraceContext = TraceContext.empty,
                trafficReceiptO = None,
                inFlightAggregation = None,
              )
            )
        )("send to unknown recipient")
        _ <- eventuallyFUS(10.seconds) {
          for {
            events <- env.store.readEvents(aliceId, alice)
            error = events.events.collectFirst {
              case Sequenced(
                    _,
                    deliverError @ DeliverErrorStoreEvent(`aliceId`, _, _, _, _),
                  ) =>
                deliverError
            }.value
          } yield {
            getErrorMessage(error.error) should include(s"Unknown recipients: $bob")
            ()
          }
        }
      } yield succeed
    }
  }

  "notifies the event signaller of writes" in withEnv() { implicit env =>
    import env.*

    val runningSequencingTimestamp = new AtomicLong(CantonTimestamp.Epoch.toProtoPrimitive)
    def deliverEvent(memberId: SequencerMemberId): Unit =
      offerDeliverOrFail(
        Presequenced.alwaysValid(
          DeliverStoreEvent.ensureSenderReceivesEvent(
            memberId,
            messageId1,
            Set.empty,
            generatePayload(),
            None,
            None,
          ),
          Some(CantonTimestamp.assertFromLong(runningSequencingTimestamp.incrementAndGet())),
        )
      )

    // get the next notification that is hopefully caused by whatever happens in the passed sequencing block
    // we've set the event batch count to 1, so we should know ahead of time how many writes this will cause
    // which allows us to collect all notifications that should be emitted during this batch
    def expectNotification(writeCount: Int)(members: SequencerMemberId*)(
        sequencingBlock: () => Unit
    ): Future[WriteNotification] = {
      val combinedNotificationsF = {
        val promise = Promise[WriteNotification]()
        val items = new AtomicReference[Seq[WriteNotification]](Seq.empty)

        val removeListener = eventSignaller.attachWriteListener { notification =>
          // ignore notifications for no-one as that's just our keep alives
          if (notification != WriteNotification.None) {
            val newItems = items.updateAndGet(_ :+ notification)

            if (newItems.sizeIs == writeCount) {
              val combined = NonEmptyUtil.fromUnsafe(newItems).reduceLeft(_ union _)
              promise.success(combined)
            }
          }
        }

        // try to not leak a callback
        promise.future transform { result =>
          removeListener.close()
          result
        }
      }

      sequencingBlock()

      combinedNotificationsF map { notification =>
        forAll(members) { member =>
          withClue(s"expecting notification for $member") {
            notification.includes(member) shouldBe true
          }
        }

        notification
      }
    }

    for {
      aliceId <- store.registerMember(alice, CantonTimestamp.Epoch).failOnShutdown
      bobId <- store.registerMember(bob, CantonTimestamp.Epoch).failOnShutdown
      // check single members
      _ <- expectNotification(writeCount = 1)(aliceId) { () =>
        deliverEvent(aliceId)
      }
      _ <- expectNotification(writeCount = 1)(bobId) { () =>
        deliverEvent(bobId)
      }
      // if multiple deliver events are written in the same batch it should union the members
      _ <- expectNotification(writeCount = 2)(aliceId, bobId) { () =>
        deliverEvent(aliceId)
        deliverEvent(bobId)
      }
    } yield succeed
  }

  "an idle writer still updates its watermark to demonstrate that its online" in withEnv(
    Some(NonNegativeFiniteDuration.tryOfSeconds(1L))
  ) { implicit env =>
    import env.*
    // the specified keepAliveInterval of 1s ensures the watermark gets updated
    for {
      initialWatermark <- eventuallyFUS(5.seconds) {
        store.fetchWatermark(instanceIndex).map(_.value)
      }
      _ <- eventuallyFUS(5.seconds) {
        for {
          updated <- store.fetchWatermark(instanceIndex)
        } yield {
          updated.value.timestamp shouldBe >=(initialWatermark.timestamp)
          ()
        }
      }
    } yield succeed
  }

  private def eventuallyF[A](timeout: FiniteDuration, checkInterval: FiniteDuration = 100.millis)(
      testCode: => Future[A]
  )(implicit env: Env): Future[A] = {
    val giveUpAt = Instant.now().plus(timeout.toMicros, ChronoUnit.MICROS)
    val resultP = Promise[A]()

    def check(): Unit = testCode.onComplete {
      case Success(value) => resultP.success(value)
      case Failure(testFailedEx: TestFailedException) =>
        // see if we can still retry
        if (Instant.now().isBefore(giveUpAt)) {
          // schedule a check later
          env.actorSystem.scheduler.scheduleOnce(checkInterval)(check())
        } else {
          // fail immediately
          resultP.failure(testFailedEx)
        }
      case Failure(otherReason) => resultP.failure(otherReason)
    }

    check()

    resultP.future
  }

  private def eventuallyFUS[A](timeout: FiniteDuration, checkInterval: FiniteDuration = 100.millis)(
      testCode: => FutureUnlessShutdown[A]
  )(implicit env: Env): FutureUnlessShutdown[A] =
    FutureUnlessShutdown.outcomeF(eventuallyF(timeout, checkInterval)(testCode.failOnShutdown))

  "periodic checkpointing" should {
    // TODO(#16087) ignore test for blockSequencerMode=false
    "produce checkpoints" in withEnv() { implicit env =>
      import env.*

      for {
        aliceId <- store.registerMember(alice, CantonTimestamp.Epoch).failOnShutdown
        _ <- store.registerMember(bob, CantonTimestamp.Epoch).failOnShutdown
        _ <- store.registerMember(charlie, CantonTimestamp.Epoch).failOnShutdown
        batch = Batch.fromClosed(
          testedProtocolVersion,
          ClosedEnvelope.create(
            ByteString.EMPTY,
            Recipients.cc(bob),
            Seq.empty,
            testedProtocolVersion,
          ),
        )
        _ <- valueOrFail(
          writer.blockSequencerWrite(
            SubmissionOutcome.Deliver(
              SubmissionRequest.tryCreate(
                alice,
                MessageId.tryCreate("test-deliver"),
                batch = batch,
                maxSequencingTime = CantonTimestamp.MaxValue,
                topologyTimestamp = None,
                aggregationRule = None,
                submissionCost = None,
                protocolVersion = testedProtocolVersion,
              ),
              sequencingTime = CantonTimestamp.Epoch.immediateSuccessor,
              deliverToMembers = Set(alice, bob),
              batch = batch,
              submissionTraceContext = TraceContext.empty,
              trafficReceiptO = None,
              inFlightAggregation = None,
            )
          )
        )("send").failOnShutdown
        eventTs <- eventuallyF(10.seconds) {
          for {
            events <- env.store.readEvents(aliceId, alice).failOnShutdown
            _ = events.events should have size 1
          } yield events.events.headOption.map(_.timestamp).valueOrFail("expected event to exist")
        }
        _ = (0 to 30).foreach { _ =>
          Threading.sleep(100L) // wait for checkpoints to be generated
          env.clock.advance(java.time.Duration.ofMillis(100))
        }
        checkpointingTs = clock.now
        checkpoints <- store.checkpointsAtTimestamp(checkpointingTs)
      } yield {
        val expectedCheckpoints = Map(
          alice -> CounterCheckpoint(SequencerCounter(0), checkpointingTs, None),
          bob -> CounterCheckpoint(SequencerCounter(0), checkpointingTs, None),
          charlie -> CounterCheckpoint(SequencerCounter(-1), checkpointingTs, None),
        )
        checkpoints should contain theSameElementsAs expectedCheckpoints
      }
    }
  }
}
