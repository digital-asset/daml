// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule, TracedLogger}
import com.digitalasset.canton.sequencing.SequencedSerializedEvent
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.synchronizer.sequencer.SynchronizerSequencingTestUtils.*
import com.digitalasset.canton.synchronizer.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.synchronizer.sequencer.store.*
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  Member,
  ParticipantId,
  SequencerGroup,
  SequencerId,
  TestingTopology,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  ProtocolVersionChecksFixtureAsyncWordSpec,
  config,
}
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, SinkQueueWithCancel, Source}
import org.apache.pekko.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}
import org.slf4j.event.Level

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}

class SequencerReaderTestV2
    extends FixtureAsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksFixtureAsyncWordSpec
    with FailOnShutdown {

  private val alice = ParticipantId("alice")
  private val bob = ParticipantId("bob")
  private val ts0 = CantonTimestamp.Epoch
  private val synchronizerId = DefaultTestIdentities.physicalSynchronizerId
  private val topologyClientMember = SequencerId(synchronizerId.uid)
  private val crypto = TestingTopology(
    sequencerGroup = SequencerGroup(
      active = Seq(SequencerId(synchronizerId.uid)),
      passive = Seq.empty,
      threshold = PositiveInt.one,
    ),
    participants = Seq(
      alice,
      bob,
    ).map((_, ParticipantAttributes(ParticipantPermission.Confirmation))).toMap,
  ).build(loggerFactory).forOwner(SequencerId(synchronizerId.uid))
  private val cryptoD =
    valueOrFail(
      crypto
        .forSynchronizer(synchronizerId.logical, defaultStaticSynchronizerParameters)
        .toRight("no crypto api")
    )(
      "synchronizer crypto"
    )
  private val instanceDiscriminator = new UUID(1L, 2L)

  class ManualEventSignaller(implicit materializer: Materializer)
      extends EventSignaller
      with FlagCloseableAsync {
    private val (queue, source) = Source
      .queue[ReadSignal](1)
      .buffer(1, OverflowStrategy.dropHead)
      .preMaterialize()

    override protected def timeouts: ProcessingTimeout = SequencerReaderTestV2.this.timeouts

    def signalRead(): Unit = queue.offer(ReadSignal).discard[QueueOfferResult]

    override def readSignalsForMember(
        member: Member,
        memberId: SequencerMemberId,
    )(implicit traceContext: TraceContext): Source[ReadSignal, NotUsed] =
      source

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
      SyncCloseable("queue", queue.complete())
    )

    override protected def logger: TracedLogger = SequencerReaderTestV2.this.logger

    override def notifyOfLocalWrite(notification: WriteNotification)(implicit
        traceContext: TraceContext
    ): Future[Unit] = Future.unit
  }

  class Env extends FlagCloseableAsync {
    protected val timeouts: ProcessingTimeout = SequencerReaderTestV2.this.timeouts
    protected val logger: TracedLogger = SequencerReaderTestV2.this.logger
    val autoPushLatestTimestamps =
      new AtomicBoolean(true) // should the latest timestamp be added to the signaller when stored
    val actorSystem: ActorSystem = ActorSystem(classOf[SequencerReaderTestV2].getSimpleName)
    implicit val materializer: Materializer = Materializer(actorSystem)
    val store = new InMemorySequencerStore(
      protocolVersion = testedProtocolVersion,
      sequencerMember = topologyClientMember,
      blockSequencerMode = true,
      loggerFactory = loggerFactory,
    )
    val instanceIndex: Int = 0
    // create a spy so we can add verifications on how many times methods were called
    val storeSpy: InMemorySequencerStore = spy[InMemorySequencerStore](store)
    val testConfig: SequencerReaderConfig =
      SequencerReaderConfig(
        readBatchSize = 10,
        checkpointInterval = config.NonNegativeFiniteDuration.ofMillis(800),
      )
    val eventSignaller = new ManualEventSignaller()
    val reader = new SequencerReader(
      testConfig,
      synchronizerId,
      storeSpy,
      cryptoD,
      eventSignaller,
      topologyClientMember,
      testedProtocolVersion,
      timeouts,
      loggerFactory,
    )
    val defaultTimeout: FiniteDuration = 20.seconds
    implicit val closeContext: CloseContext = CloseContext(reader)

    def ts(epochSeconds: Int): CantonTimestamp = CantonTimestamp.ofEpochSecond(epochSeconds.toLong)

    /** Can be used at most once per environment because
      * [[org.apache.pekko.stream.scaladsl.FlowOps.take]] cancels the pre-materialized
      * [[ManualEventSignaller.source]].
      */
    def readAsSeq(
        member: Member,
        timestampInclusive: Option[CantonTimestamp],
        take: Int,
    ): FutureUnlessShutdown[Seq[SequencedSerializedEvent]] =
      loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.WARN))(
        FutureUnlessShutdown.outcomeF(
          valueOrFail(reader.readV2(member, timestampInclusive).failOnShutdown)(
            s"Events source for $member"
          ) flatMap { eventSource =>
            eventSource
              .take(take.toLong)
              .idleTimeout(defaultTimeout)
              .map {
                case Right(event) => event
                case Left(err) =>
                  fail(
                    s"The DatabaseSequencer's SequencerReader does not produce tombstone-errors: $err"
                  )
              }
              .runWith(Sink.seq)
          }
        ),
        ignoreWarningsFromLackOfTopologyUpdates,
      )

    def readWithQueueFUS(
        member: Member,
        timestampInclusive: Option[CantonTimestamp],
    ): FutureUnlessShutdown[SinkQueueWithCancel[SequencedSerializedEvent]] = {

      val subscribeF = valueOrFail(reader.readV2(member, timestampInclusive).failOnShutdown)(
        s"Events source for $member"
      )

      val source = Source
        .future(
          subscribeF
        )
        .flatMapConcat(identity)
        .map {
          case Right(event) => event
          case Left(err) =>
            fail(s"The DatabaseSequencer's SequencerReader does not produce tombstone-errors: $err")
        }
        .idleTimeout(defaultTimeout)
        .runWith(Sink.queue())

      FutureUnlessShutdown.outcomeF(subscribeF.map(_ => source))
    }

    def readWithQueue(
        member: Member,
        timestampInclusive: Option[CantonTimestamp],
    ): SinkQueueWithCancel[SequencedSerializedEvent] =
      Source
        .future(
          valueOrFail(reader.readV2(member, timestampInclusive).failOnShutdown)(
            s"Events source for $member"
          )
        )
        .flatMapConcat(identity)
        .map {
          case Right(event) => event
          case Left(err) =>
            fail(s"The DatabaseSequencer's SequencerReader does not produce tombstone-errors: $err")
        }
        .idleTimeout(defaultTimeout)
        .runWith(Sink.queue())

    // We don't update the topology client, so we expect to get a couple of warnings about unknown topology snapshots
    private def ignoreWarningsFromLackOfTopologyUpdates(entries: Seq[LogEntry]): Assertion =
      forEvery(entries) {
        _.warningMessage should fullyMatch regex ".*Using approximate topology snapshot .* for desired timestamp.*"
      }

    def pullFromQueue(
        queue: SinkQueueWithCancel[SequencedSerializedEvent]
    ): FutureUnlessShutdown[Option[SequencedSerializedEvent]] =
      loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.WARN))(
        FutureUnlessShutdown.outcomeF(queue.pull()),
        ignoreWarningsFromLackOfTopologyUpdates,
      )

    def waitFor(duration: FiniteDuration): FutureUnlessShutdown[Unit] =
      FutureUnlessShutdown.outcomeF {
        val promise = Promise[Unit]()

        actorSystem.scheduler.scheduleOnce(duration)(promise.success(()))

        promise.future
      }

    def storeAndWatermark(events: Seq[Sequenced[PayloadId]]): FutureUnlessShutdown[Unit] = {
      val withPaylaods = events.map(
        _.map(id => BytesPayload(id, Batch.empty(testedProtocolVersion).toByteString))
      )
      storePayloadsAndWatermark(withPaylaods)
    }

    def storePayloadsAndWatermark(
        events: Seq[Sequenced[BytesPayload]]
    ): FutureUnlessShutdown[Unit] = {
      val eventsNE = NonEmptyUtil.fromUnsafe(events.map(_.map(_.id)))
      val payloads = NonEmpty.from(events.mapFilter(_.event.payloadO))

      for {
        _ <- payloads
          .traverse_(store.savePayloads(_, instanceDiscriminator))
          .valueOrFail("Save payloads")
        _ <- store.saveEvents(instanceIndex, eventsNE)
        _ <- store
          .saveWatermark(instanceIndex, eventsNE.last1.timestamp)
          .valueOrFail("saveWatermark")
      } yield {
        // update the event signaller if auto signalling is enabled
        if (autoPushLatestTimestamps.get()) eventSignaller.signalRead()
      }
    }

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
      AsyncCloseable(
        "actorSystem",
        actorSystem.terminate(),
        config.NonNegativeFiniteDuration(10.seconds),
      ),
      SyncCloseable("materializer", materializer.shutdown()),
    )
  }

  override type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()

    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  "Reader" should {
    "read a stream of events" in { env =>
      import env.*

      for {
        _ <- store.registerMember(topologyClientMember, ts0).failOnShutdown
        aliceId <- store.registerMember(alice, ts0).failOnShutdown
        // generate 20 delivers starting at ts0+1s
        events = (1L to 20L)
          .map(ts0.plusSeconds)
          .map(Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)()))
        _ <- storeAndWatermark(events)
        events <- readAsSeq(alice, timestampInclusive = None, 20)
      } yield {
        forAll(events.zipWithIndex) { case (event, n) =>
          val expectedPreviousEventTimestamp = if (n == 0) None else Some(ts0.plusSeconds(n.toLong))
          event.timestamp shouldBe ts0.plusSeconds(n + 1L)
          event.previousTimestamp shouldBe expectedPreviousEventTimestamp
        }
      }
    }

    "read a stream of events from a non-zero offset" in { env =>
      import env.*

      for {
        _ <- store.registerMember(topologyClientMember, ts0).failOnShutdown
        aliceId <- store.registerMember(alice, ts0).failOnShutdown
        delivers = (1L to 20L)
          .map(ts0.plusSeconds)
          .map(Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)()))
          .toList
        _ <- storeAndWatermark(delivers)
        events <- readAsSeq(alice, Some(ts0.plusSeconds(6)), 15)
      } yield {
        events.headOption.value.previousTimestamp shouldBe Some(ts0.plusSeconds(5))
        events.headOption.value.timestamp shouldBe ts0.plusSeconds(6)
        events.lastOption.value.previousTimestamp shouldBe Some(ts0.plusSeconds(19))
        events.lastOption.value.timestamp shouldBe ts0.plusSeconds(20)
      }
    }

    "read stream of events while new events are being added" in { env =>
      import env.*

      for {
        _ <- store.registerMember(topologyClientMember, ts0).failOnShutdown
        aliceId <- store.registerMember(alice, ts0).failOnShutdown
        delivers = (1L to 5L)
          .map(ts0.plusSeconds)
          .map(Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)()))
          .toList
        _ <- storeAndWatermark(delivers)
        queue = readWithQueue(alice, timestampInclusive = None)
        // read off all of the initial delivers
        _ <- MonadUtil.sequentialTraverse(delivers.zipWithIndex.map(_._2)) { idx =>
          for {
            eventO <- pullFromQueue(queue)
          } yield eventO.value.timestamp shouldBe ts0.plusSeconds(idx + 1L)
        }
        // start reading the next event
        nextEventF = pullFromQueue(queue)
        // add another
        _ <- storeAndWatermark(
          Seq(
            Sequenced(
              ts0.plusSeconds(6L),
              mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)(),
            )
          )
        )
        // wait for the next event
        nextEventO <- nextEventF
        _ = queue.cancel() // cancel the queue now we're done with it
      } yield {
        nextEventO.value.previousTimestamp shouldBe Some(ts0.plusSeconds(5))
        nextEventO.value.timestamp shouldBe ts0.plusSeconds(6)
      } // it'll be alices fifth event
    }

    "correctly compute previous timestamp when subscribing above the watermark" in { env =>
      import env.*

      for {
        _ <- store.registerMember(topologyClientMember, ts0).failOnShutdown
        aliceId <- store.registerMember(alice, ts0).failOnShutdown
        delivers = (1L to 5L)
          .map(ts0.plusSeconds)
          .map(Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)()))
          .toList
        _ <- storeAndWatermark(delivers)

        // We simulate the processing being slow (still processing ts=2 when subscription is created for ts=5)
        _ <- store.saveWatermark(instanceIndex, ts0.plusSeconds(2L)).valueOrFail("saveWatermark")
        // We create a subscription for ts=5 (above the watermark)
        queue <- readWithQueueFUS(alice, timestampInclusive = ts0.plusSeconds(5L).some)
        // We advance the watermark, so that ts=5 event can be read
        _ <- store.saveWatermark(instanceIndex, ts0.plusSeconds(5L)).valueOrFail("saveWatermark")

        event5 <- pullFromQueue(queue)
        _ = queue.cancel() // cancel the queue now we're done with it
      } yield {
        event5.value.previousTimestamp shouldBe Some(ts0.plusSeconds(4L))
        event5.value.timestamp shouldBe ts0.plusSeconds(5L)
      }
    }

    "attempting to read an unregistered member returns error" in { env =>
      import env.*

      for {
        _ <- store.registerMember(topologyClientMember, ts0)
        // we haven't registered alice
        error <- leftOrFail(reader.readV2(alice, requestedTimestampInclusive = None))(
          "read unknown member"
        )
      } yield error shouldBe CreateSubscriptionError.UnknownMember(alice)
    }

    "attempting to read without having registered the topology client member returns error" in {
      env =>
        import env.*
        for {
          // we haven't registered the topology client member
          _ <- store.registerMember(alice, ts0)
          error <- leftOrFail(reader.readV2(alice, requestedTimestampInclusive = None))(
            "read unknown topology client"
          )
        } yield error shouldBe CreateSubscriptionError.UnknownMember(topologyClientMember)
    }

    "attempting to read for a disabled member returns error" in { env =>
      import env.*

      for {
        _ <- store.registerMember(topologyClientMember, ts0)
        _ <- store.registerMember(alice, ts0)
        _ <- store.disableMember(alice)
        error <- leftOrFail(reader.readV2(alice, requestedTimestampInclusive = None))(
          "read disabled member"
        )
      } yield error shouldBe CreateSubscriptionError.MemberDisabled(alice)
    }

    "waits for a signal that new events are available" in { env =>
      import env.*

      val waitP = Promise[Unit]()

      for {
        _ <- store.registerMember(topologyClientMember, ts0).failOnShutdown
        aliceId <- store.registerMember(alice, ts0).failOnShutdown
        // start reading for an event but don't wait for it
        eventsF = readAsSeq(alice, timestampInclusive = None, 1)
        // set a timer to wait for a little
        _ = actorSystem.scheduler.scheduleOnce(500.millis)(waitP.success(()))
        // still shouldn't have read anything
        _ = eventsF.isCompleted shouldBe false
        // now signal that events are available which should cause the future read to move ahead
        _ = env.eventSignaller.signalRead()
        _ <- waitP.future
        // add an event
        _ <- storeAndWatermark(
          Seq(
            Sequenced(
              ts0 plusSeconds 1,
              mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)(),
            )
          )
        )
        _ = env.eventSignaller.signalRead() // signal that something is there
        events <- eventsF
      } yield {
        events should have size 1 // should have got our single deliver event
      }
    }

    "reading all immediately available events" should {
      "use returned events before filtering based what has actually been requested" in { env =>
        import env.*

        // disable auto signalling
        autoPushLatestTimestamps.set(false)

        for {
          _ <- store.registerMember(topologyClientMember, ts0)
          aliceId <- store.registerMember(alice, ts0)
          // generate 25 delivers starting at ts0+1s
          delivers = (1L to 25L)
            .map(ts0.plusSeconds)
            .map(
              Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
            )
          _ <- storeAndWatermark(delivers)
          events <- readAsSeq(alice, timestampInclusive = Some(ts0.plusSeconds(11)), 15)
        } yield {
          // this assertion is a bit redundant as we're actually just looking for the prior fetch to complete rather than get stuck
          events should have size 15
          events.headOption.value.previousTimestamp shouldBe Some(ts0.plusSeconds(10))
          events.headOption.value.timestamp shouldBe ts0.plusSeconds(11)
          events.lastOption.value.previousTimestamp shouldBe Some(ts0.plusSeconds(24))
          events.lastOption.value.timestamp shouldBe ts0.plusSeconds(25)
        }
      }
    }

    "lower bound checks" should {
      "error if subscription would need to start before the lower bound due to no checkpoints" in {
        env =>
          import env.*

          val expectedMessage =
            "Subscription for PAR::alice::default would require reading data from the beginning, but this sequencer cannot serve timestamps at or before 1970-01-01T00:00:10Z or below the member's registration timestamp 1970-01-01T00:00:00Z."

          for {
            _ <- store.registerMember(topologyClientMember, ts0).failOnShutdown
            aliceId <- store.registerMember(alice, ts0).failOnShutdown
            // write a bunch of events
            delivers = (1L to 20L)
              .map(ts0.plusSeconds)
              .map(
                Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
              )
            _ <- storeAndWatermark(delivers)
            _ <- store
              .saveLowerBound(ts(10), ts(9).some)
              .valueOrFail("saveLowerBound")
            error <- loggerFactory.assertLogs(
              leftOrFail(reader.readV2(alice, requestedTimestampInclusive = None))("read"),
              _.errorMessage shouldBe expectedMessage,
            )
          } yield inside(error) {
            case CreateSubscriptionError.EventsUnavailableForTimestamp(None, message) =>
              message should include(expectedMessage)
          }
      }

      "error if subscription would need to start before the lower bound" in { env =>
        import env.*

        val expectedMessage =
          "Subscription for PAR::alice::default would require reading data from 1970-01-01T00:00:10Z (inclusive), but this sequencer cannot serve timestamps at or before 1970-01-01T00:00:10Z or below the member's registration timestamp 1970-01-01T00:00:00Z."

        for {
          _ <- store.registerMember(topologyClientMember, ts0).failOnShutdown
          aliceId <- store.registerMember(alice, ts0).failOnShutdown
          // write a bunch of events
          delivers = (1L to 20L)
            .map(ts0.plusSeconds)
            .map(
              Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
            )
          _ <- storeAndWatermark(delivers)
          _ <- store
            .saveLowerBound(ts(10), ts(9).some)
            .valueOrFail("saveLowerBound")
          error <- loggerFactory.assertLogs(
            leftOrFail(
              reader.readV2(alice, requestedTimestampInclusive = Some(ts0.plusSeconds(10)))
            )(
              "read succeeded"
            ),
            _.errorMessage shouldBe expectedMessage,
          )
        } yield inside(error) {
          case CreateSubscriptionError.EventsUnavailableForTimestamp(Some(timestamp), message) =>
            timestamp shouldBe ts0.plusSeconds(10)
            message shouldBe expectedMessage
        }
      }

      "not error if reading data above lower bound" in { env =>
        import env.*

        for {
          _ <- store.registerMember(topologyClientMember, ts0).failOnShutdown
          aliceId <- store.registerMember(alice, ts0).failOnShutdown
          // write a bunch of events
          delivers = (1L to 20L)
            .map(ts0.plusSeconds)
            .map(Sequenced(_, mockDeliverStoreEvent(sender = aliceId)()))
          _ <- storeAndWatermark(delivers)
          _ <- store
            .saveLowerBound(ts(10), ts(9).some)
            .valueOrFail("saveLowerBound")
          _ <- reader
            .readV2(alice, requestedTimestampInclusive = Some(ts0.plusSeconds(13)))
            .valueOrFail("read")
        } yield succeed // the above not failing is enough of an assertion
      }
    }

    "convert deliver events with too-old signing timestamps" when {

      def setup(env: Env) = {
        import env.*

        for {
          synchronizerParamsO <- cryptoD.headSnapshot.ipsSnapshot
            .findDynamicSynchronizerParameters()
          synchronizerParams = synchronizerParamsO.valueOrFail("No synchronizer parameters found")
          topologyTimestampTolerance = synchronizerParams.sequencerTopologyTimestampTolerance
          topologyTimestampToleranceInSec = topologyTimestampTolerance.duration.toSeconds

          _ <- store.registerMember(topologyClientMember, ts0)
          aliceId <- store.registerMember(alice, ts0)
          bobId <- store.registerMember(bob, ts0)

          recipients = NonEmpty(SortedSet, aliceId, bobId)
          testData: Seq[(Option[Long], Long, Long)] = Seq(
            // Previous ts, sequencing ts, signing ts relative to ts0
            (None, 1L, 0L),
            (Some(1), topologyTimestampToleranceInSec, 0L),
            (Some(topologyTimestampToleranceInSec), topologyTimestampToleranceInSec + 1L, 0L),
            (Some(topologyTimestampToleranceInSec + 1L), topologyTimestampToleranceInSec + 2L, 2L),
          )
          batch = Batch.fromClosed(
            testedProtocolVersion,
            ClosedEnvelope.create(
              ByteString.copyFromUtf8("test envelope"),
              Recipients.cc(alice, bob),
              Seq.empty,
              testedProtocolVersion,
            ),
          )

          delivers = testData.map { case (_, sequenceTs, signingTs) =>
            val storeEvent = TraceContext
              .withNewTraceContext { eventTraceContext =>
                mockDeliverStoreEvent(
                  sender = aliceId,
                  payloadId = PayloadId(ts0.plusSeconds(sequenceTs)),
                  signingTs = Some(ts0.plusSeconds(signingTs)),
                  traceContext = eventTraceContext,
                )(recipients)
              }
              .map(id => BytesPayload(id, batch.toByteString))
            Sequenced(ts0.plusSeconds(sequenceTs), storeEvent)
          }
          previousTimestamps = testData.map { case (previousTs, _, _) =>
            previousTs.map(ts0.plusSeconds)
          }
          _ <- storePayloadsAndWatermark(delivers)
        } yield (topologyTimestampTolerance, batch, delivers, previousTimestamps)
      }

      final case class DeliveredEventToCheck[A](
          delivered: A,
          previousTimestamp: Option[CantonTimestamp],
          sequencingTimestamp: CantonTimestamp,
          messageId: MessageId,
          topologyTimestamp: CantonTimestamp,
      )

      def filterForTopologyTimestamps[A]: PartialFunction[
        (((A, Sequenced[BytesPayload]), Int), Option[CantonTimestamp]),
        DeliveredEventToCheck[A],
      ] = {
        case (
              (
                (
                  delivered,
                  Sequenced(
                    timestamp,
                    DeliverStoreEvent(
                      _sender,
                      messageId,
                      _members,
                      _payload,
                      Some(topologyTimestamp),
                      _traceContext,
                      _trafficReceiptO,
                    ),
                  ),
                ),
                _idx,
              ),
              previousTimestamp,
            ) =>
          DeliveredEventToCheck(
            delivered,
            previousTimestamp,
            timestamp,
            messageId,
            topologyTimestamp,
          )
      }

      "read by the sender into deliver errors" in { env =>
        import env.*
        setup(env).flatMap {
          case (topologyTimestampTolerance, batch, delivers, previousTimestamps) =>
            for {
              aliceEvents <- readAsSeq(alice, timestampInclusive = None, delivers.length)
            } yield {
              aliceEvents.length shouldBe delivers.length
              val deliverWithTopologyTimestamps =
                aliceEvents.zip(delivers).zipWithIndex.zip(previousTimestamps).collect {
                  filterForTopologyTimestamps
                }
              forEvery(deliverWithTopologyTimestamps) {
                case DeliveredEventToCheck(
                      delivered,
                      previousTimestamp,
                      sequencingTimestamp,
                      messageId,
                      topologyTimestamp,
                    ) =>
                  val expectedSequencedEvent =
                    if (topologyTimestamp + topologyTimestampTolerance >= sequencingTimestamp)
                      Deliver.create(
                        previousTimestamp,
                        sequencingTimestamp,
                        synchronizerId,
                        messageId.some,
                        batch,
                        Some(topologyTimestamp),
                        testedProtocolVersion,
                        Option.empty[TrafficReceipt],
                      )
                    else
                      DeliverError.create(
                        previousTimestamp,
                        sequencingTimestamp,
                        synchronizerId,
                        messageId,
                        SequencerErrors.TopologyTimestampTooEarly(
                          topologyTimestamp,
                          sequencingTimestamp,
                        ),
                        testedProtocolVersion,
                        Option.empty[TrafficReceipt],
                      )
                  delivered.signedEvent.content shouldBe expectedSequencedEvent
              }
            }
        }
      }

      "read by another recipient into empty batches" in { env =>
        import env.*
        setup(env).flatMap {
          case (topologyTimestampTolerance, batch, delivers, previousTimestamps) =>
            for {
              bobEvents <- readAsSeq(bob, timestampInclusive = None, delivers.length)
            } yield {
              bobEvents.length shouldBe delivers.length
              val deliverWithTopologyTimestamps =
                bobEvents.zip(delivers).zipWithIndex.zip(previousTimestamps).collect {
                  filterForTopologyTimestamps
                }
              forEvery(deliverWithTopologyTimestamps) {
                case DeliveredEventToCheck(
                      delivered,
                      previousTimestamp,
                      sequencingTimestamp,
                      _messageId,
                      topologyTimestamp,
                    ) =>
                  val expectedSequencedEvent =
                    if (topologyTimestamp + topologyTimestampTolerance >= sequencingTimestamp)
                      Deliver.create(
                        previousTimestamp,
                        sequencingTimestamp,
                        synchronizerId,
                        None,
                        batch,
                        Some(topologyTimestamp),
                        testedProtocolVersion,
                        Option.empty[TrafficReceipt],
                      )
                    else
                      Deliver.create(
                        previousTimestamp,
                        sequencingTimestamp,
                        synchronizerId,
                        None,
                        Batch.empty(testedProtocolVersion),
                        None,
                        testedProtocolVersion,
                        Option.empty[TrafficReceipt],
                      )
                  delivered.signedEvent.content shouldBe expectedSequencedEvent
              }
            }
        }
      }
    }
  }
}
