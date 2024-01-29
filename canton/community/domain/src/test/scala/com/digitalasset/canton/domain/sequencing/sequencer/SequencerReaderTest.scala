// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.DomainSequencingTestUtils.*
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.domain.sequencing.sequencer.store.*
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  CloseContext,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule, TracedLogger}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  Deliver,
  DeliverError,
  MessageId,
  Recipients,
  SequencerErrors,
}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  Member,
  ParticipantId,
  SequencerId,
  TestingTopology,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{BaseTest, DiscardOps, SequencerCounter, config}
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, SinkQueueWithCancel, Source}
import org.apache.pekko.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import org.mockito.Mockito
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}
import org.slf4j.event.Level

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}

class SequencerReaderTest extends FixtureAsyncWordSpec with BaseTest {

  private val alice: Member = ParticipantId("alice")
  private val bob: Member = ParticipantId("bob")
  private val ts0 = CantonTimestamp.Epoch
  private val domainId = DefaultTestIdentities.domainId
  private val topologyClientMember = SequencerId(domainId)
  private val crypto = TestingTopology().build(loggerFactory).forOwner(SequencerId(domainId))
  private val cryptoD =
    valueOrFail(crypto.forDomain(domainId).toRight("no crypto api"))("domain crypto")
  private val instanceDiscriminator = new UUID(1L, 2L)

  class ManualEventSignaller(implicit materializer: Materializer)
      extends EventSignaller
      with FlagCloseableAsync {
    private val (queue, source) = Source
      .queue[ReadSignal](1)
      .buffer(1, OverflowStrategy.dropHead)
      .preMaterialize()

    override protected def timeouts: ProcessingTimeout = SequencerReaderTest.this.timeouts

    def signalRead(): Unit = queue.offer(ReadSignal).discard[QueueOfferResult]

    override def readSignalsForMember(
        member: Member,
        memberId: SequencerMemberId,
    )(implicit traceContext: TraceContext): Source[ReadSignal, NotUsed] =
      source

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
      SyncCloseable("queue", queue.complete())
    )

    override protected def logger: TracedLogger = SequencerReaderTest.this.logger

    override def notifyOfLocalWrite(notification: WriteNotification)(implicit
        traceContext: TraceContext
    ): Future[Unit] = Future.unit
  }

  class Env extends FlagCloseableAsync {
    protected val timeouts = SequencerReaderTest.this.timeouts
    protected val logger = SequencerReaderTest.this.logger
    val autoPushLatestTimestamps =
      new AtomicBoolean(true) // should the latest timestamp be added to the signaller when stored
    val actorSystem = ActorSystem(classOf[SequencerReaderTest].getSimpleName)
    implicit val materializer: Materializer = Materializer(actorSystem)
    val store = new InMemorySequencerStore(loggerFactory)
    val instanceIndex: Int = 0
    // create a spy so we can add verifications on how many times methods were called
    val storeSpy = spy[InMemorySequencerStore](store)
    val testConfig =
      CommunitySequencerReaderConfig(
        readBatchSize = 10,
        checkpointInterval = config.NonNegativeFiniteDuration.ofMillis(800),
      )
    val eventSignaller = new ManualEventSignaller()
    val reader = new SequencerReader(
      testConfig,
      domainId,
      storeSpy,
      cryptoD,
      eventSignaller,
      topologyClientMember,
      testedProtocolVersion,
      timeouts,
      loggerFactory,
    )
    val defaultTimeout = 20.seconds
    implicit val closeContext: CloseContext = CloseContext(reader)

    def ts(epochSeconds: Int): CantonTimestamp = CantonTimestamp.ofEpochSecond(epochSeconds.toLong)

    /** Can be used at most once per environment because [[org.apache.pekko.stream.scaladsl.FlowOps.take]]
      * cancels the pre-materialized [[ManualEventSignaller.source]].
      */
    def readAsSeq(
        member: Member,
        sc: SequencerCounter,
        take: Int,
    ): Future[Seq[OrdinarySerializedEvent]] =
      loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.WARN))(
        valueOrFail(reader.read(member, sc))(s"Events source for $member") flatMap {
          _.take(take.toLong)
            .idleTimeout(defaultTimeout)
            .map {
              case Right(event) => event
              case Left(err) =>
                fail(
                  s"The DatabaseSequencer's SequencerReader does not produce tombstone-errors: $err"
                )
            }
            .runWith(Sink.seq)
        },
        ignoreWarningsFromLackOfTopologyUpdates,
      )

    def readWithQueue(
        member: Member,
        counter: SequencerCounter,
    ): SinkQueueWithCancel[OrdinarySerializedEvent] =
      Source
        .future(valueOrFail(reader.read(member, counter))(s"Events source for $member"))
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
        _.warningMessage should fullyMatch regex (".*Using approximate topology snapshot .* for desired timestamp.*")
      }

    def pullFromQueue(
        queue: SinkQueueWithCancel[OrdinarySerializedEvent]
    ): Future[Option[OrdinarySerializedEvent]] = {
      loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.WARN))(
        queue.pull(),
        ignoreWarningsFromLackOfTopologyUpdates,
      )
    }

    def waitFor(duration: FiniteDuration): Future[Unit] = {
      val promise = Promise[Unit]()

      actorSystem.scheduler.scheduleOnce(duration)(promise.success(()))

      promise.future
    }

    def storeAndWatermark(events: Seq[Sequenced[PayloadId]]): Future[Unit] = {
      val withPaylaods = events.map(
        _.map(id => Payload(id, Batch.empty(testedProtocolVersion).toByteString))
      )
      storePayloadsAndWatermark(withPaylaods)
    }

    def storePayloadsAndWatermark(events: Seq[Sequenced[Payload]]): Future[Unit] = {
      val eventsNE = NonEmptyUtil.fromUnsafe(events.map(_.map(_.id)))
      val payloads = NonEmpty.from(events.mapFilter(_.event.payloadO))

      for {
        _ <- payloads
          .traverse_(store.savePayloads(_, instanceDiscriminator))
          .valueOrFail(s"Save payloads")
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
      AsyncCloseable("actorSystem", actorSystem.terminate(), 10.seconds),
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

  private def checkpoint(
      counter: SequencerCounter,
      ts: CantonTimestamp,
      latestTopologyClientTs: Option[CantonTimestamp] = None,
  ): CounterCheckpoint =
    CounterCheckpoint(counter, ts, latestTopologyClientTs)

  "Reader" should {
    "read a stream of events" in { env =>
      import env.*

      for {
        _ <- store.registerMember(topologyClientMember, ts0)
        aliceId <- store.registerMember(alice, ts0)
        // generate 20 delivers starting at ts0+1s
        events = (1L to 20L)
          .map(ts0.plusSeconds)
          .map(Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)()))
        _ <- storeAndWatermark(events)
        events <- readAsSeq(alice, SequencerCounter(0), 20)
      } yield {
        forAll(events.zipWithIndex) { case (event, n) =>
          event.counter shouldBe SequencerCounter(n)
        }
      }
    }

    "read a stream of events from a non-zero offset" in { env =>
      import env.*

      for {
        _ <- store.registerMember(topologyClientMember, ts0)
        aliceId <- store.registerMember(alice, ts0)
        delivers = (1L to 20L)
          .map(ts0.plusSeconds)
          .map(Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)()))
          .toList
        _ <- storeAndWatermark(delivers)
        events <- readAsSeq(alice, SequencerCounter(5), 15)
      } yield {
        events.headOption.value.counter shouldBe SequencerCounter(5)
        events.headOption.value.timestamp shouldBe ts0.plusSeconds(6)
        events.lastOption.value.counter shouldBe SequencerCounter(19)
        events.lastOption.value.timestamp shouldBe ts0.plusSeconds(20)
      }
    }

    "read stream of events while new events are being added" in { env =>
      import env.*

      for {
        _ <- store.registerMember(topologyClientMember, ts0)
        aliceId <- store.registerMember(alice, ts0)
        delivers = (1L to 5L)
          .map(ts0.plusSeconds)
          .map(Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)()))
          .toList
        _ <- storeAndWatermark(delivers)
        queue = readWithQueue(alice, SequencerCounter(0))
        // read off all of the initial delivers
        _ <- MonadUtil.sequentialTraverse_(delivers.zipWithIndex.map(_._2)) { expectedCounter =>
          for {
            eventO <- pullFromQueue(queue)
          } yield eventO.value.counter shouldBe SequencerCounter(expectedCounter)
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
      } yield nextEventO.value.counter shouldBe SequencerCounter(5) // it'll be alices fifth event
    }

    "attempting to read an unregistered member returns error" in { env =>
      import env.*

      for {
        _ <- store.registerMember(topologyClientMember, ts0)
        // we haven't registered alice
        error <- leftOrFail(reader.read(alice, SequencerCounter(0)))("read unknown member")
      } yield error shouldBe CreateSubscriptionError.UnknownMember(alice)
    }

    "attempting to read without having registered the topology client member returns error" in {
      env =>
        import env.*
        for {
          // we haven't registered the topology client member
          _ <- store.registerMember(alice, ts0)
          error <- leftOrFail(reader.read(alice, SequencerCounter(0)))(
            "read unknown topology client"
          )
        } yield error shouldBe CreateSubscriptionError.UnknownMember(topologyClientMember)
    }

    "attempting to read for a disabled member returns error" in { env =>
      import env.*

      for {
        _ <- store.registerMember(topologyClientMember, ts0)
        aliceId <- store.registerMember(alice, ts0)
        _ <- store.disableMember(aliceId)
        error <- leftOrFail(reader.read(alice, SequencerCounter(0)))("read disabled member")
      } yield error shouldBe CreateSubscriptionError.MemberDisabled(alice)
    }

    "waits for a signal that new events are available" in { env =>
      import env.*

      val waitP = Promise[Unit]()

      for {
        _ <- store.registerMember(topologyClientMember, ts0)
        aliceId <- store.registerMember(alice, ts0)
        // start reading for an event but don't wait for it
        eventsF = readAsSeq(alice, SequencerCounter(0), 1)
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
          // generate 20 delivers starting at ts0+1s
          delivers = (1L to 25L)
            .map(ts0.plusSeconds)
            .map(
              Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
            )
          _ <- storeAndWatermark(delivers)
          // store a counter check point at 5s
          _ <- store
            .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(5), ts(6)))
            .valueOrFail("saveCounterCheckpoint")
          events <- readAsSeq(alice, SequencerCounter(10), 15)
        } yield {
          // this assertion is a bit redundant as we're actually just looking for the prior fetch to complete rather than get stuck
          events should have size (15)
        }
      }
    }

    "counter checkpoint" should {
      "issue counter checkpoints occasionally" in { env =>
        import env.*

        import scala.jdk.CollectionConverters.*

        def saveCounterCheckpointCallCount =
          Mockito
            .mockingDetails(storeSpy)
            .getInvocations
            .asScala
            .count(_.getMethod.getName == "saveCounterCheckpoint")

        for {
          topologyClientMemberId <- store.registerMember(topologyClientMember, ts0)
          aliceId <- store.registerMember(alice, ts0)
          // generate 20 delivers starting at ts0+1s
          delivers = (1L to 20L).map { i =>
            val recipients =
              if (i == 1L || i == 11L) NonEmpty(SortedSet, topologyClientMemberId, aliceId)
              else NonEmpty(SortedSet, aliceId)
            Sequenced(
              ts0.plusSeconds(i),
              mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)(recipients),
            )
          }
          _ <- storeAndWatermark(delivers)
          start = System.nanoTime()
          // take some events
          queue = readWithQueue(alice, SequencerCounter(0))
          // read a bunch of items
          readEvents <- MonadUtil.sequentialTraverse(1L to 20L)(_ => pullFromQueue(queue))
          // wait for a bit over the checkpoint interval (although I would expect because these actions are using the same scheduler the actions may be correctly ordered regardless)
          _ <- waitFor(testConfig.checkpointInterval.underlying * 6)
          checkpointsWritten = saveCounterCheckpointCallCount
          stop = System.nanoTime()
          // close the queue before we make any assertions
          _ = queue.cancel()
          lastEventRead = readEvents.lastOption.value.value
          checkpointForLastEventO <- store.fetchClosestCheckpointBefore(
            aliceId,
            lastEventRead.counter + 1,
          )
        } yield {
          // check it created a checkpoint for the last event we read
          checkpointForLastEventO.value.counter shouldBe lastEventRead.counter
          checkpointForLastEventO.value.timestamp shouldBe lastEventRead.timestamp
          checkpointForLastEventO.value.latestTopologyClientTimestamp shouldBe Some(
            CantonTimestamp.ofEpochSecond(11)
          )

          val readingDurationMillis = java.time.Duration.ofNanos(stop - start).toMillis
          val checkpointsUpperBound = (readingDurationMillis.toFloat /
            testConfig.checkpointInterval.duration.toMillis.toFloat).ceil.toInt
          logger.debug(
            s"Expecting at most $checkpointsUpperBound checkpoints because reading overall took at most $readingDurationMillis ms"
          )
          // make sure we didn't write a checkpoint for every event (in practice this should be <3)
          checkpointsWritten should (be > 0 and be <= checkpointsUpperBound)
          // The next assertion fails if the test takes too long. Increase the checkpoint interval in `testConfig` if necessary.
          checkpointsUpperBound should be < 20
        }
      }

      "start subscriptions from the closest counter checkpoint if available" in { env =>
        import env.*

        for {
          _ <- store.registerMember(topologyClientMember, ts0)
          aliceId <- store.registerMember(alice, ts0)
          // write a bunch of events
          delivers = (1L to 20L)
            .map(ts0.plusSeconds)
            .map(
              Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
            )
          _ <- storeAndWatermark(delivers)
          checkpointTimestamp = ts0.plusSeconds(11)
          _ <- valueOrFail(
            store.saveCounterCheckpoint(
              aliceId,
              checkpoint(SequencerCounter(10), checkpointTimestamp),
            )
          )("saveCounterCheckpoint")
          // read from a point ahead of this checkpoint
          events <- readAsSeq(alice, SequencerCounter(15), 3)
        } yield {
          // it should have started reading from the closest counter checkpoint timestamp
          verify(storeSpy).readEvents(eqTo(aliceId), eqTo(Some(checkpointTimestamp)), anyInt)(
            anyTraceContext
          )
          // but only emitted events starting from 15
          events.headOption.value.counter shouldBe SequencerCounter(15)
          // our deliver events start at ts0+1s and as alice is registered before the first deliver event their first
          // event (0) is for ts0+1s.
          // event 15 should then have ts ts0+16s
          events.headOption.value.timestamp shouldBe ts0.plusSeconds(16)
        }
      }
    }

    "lower bound checks" should {
      "error if subscription would need to start before the lower bound due to no checkpoints" in {
        env =>
          import env.*

          val expectedMessage =
            "Subscription for PAR::alice::default@0 would require reading data from 1970-01-01T00:00:00Z but our lower bound is 1970-01-01T00:00:10Z."

          for {
            _ <- store.registerMember(topologyClientMember, ts0)
            aliceId <- store.registerMember(alice, ts0)
            // write a bunch of events
            delivers = (1L to 20L)
              .map(ts0.plusSeconds)
              .map(
                Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
              )
            _ <- storeAndWatermark(delivers)
            _ <- store.saveLowerBound(ts(10)).valueOrFail("saveLowerBound")
            error <- loggerFactory.assertLogs(
              leftOrFail(reader.read(alice, SequencerCounter(0)))("read"),
              _.errorMessage shouldBe expectedMessage,
            )
          } yield inside(error) {
            case CreateSubscriptionError.EventsUnavailable(SequencerCounter(0), message) =>
              message should include(expectedMessage)
          }
      }

      "error if subscription would need to start before the lower bound due to checkpoints" in {
        env =>
          import env.*

          val expectedMessage =
            "Subscription for PAR::alice::default@9 would require reading data from 1970-01-01T00:00:00Z but our lower bound is 1970-01-01T00:00:10Z."

          for {
            _ <- store.registerMember(topologyClientMember, ts0)
            aliceId <- store.registerMember(alice, ts0)
            // write a bunch of events
            delivers = (1L to 20L)
              .map(ts0.plusSeconds)
              .map(
                Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
              )
            _ <- storeAndWatermark(delivers)
            _ <- store
              .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(9), ts(10)))
              .valueOrFail("saveCounterCheckpoint")
            _ <- store.saveLowerBound(ts(10)).valueOrFail("saveLowerBound")
            error <- loggerFactory.assertLogs(
              leftOrFail(reader.read(alice, SequencerCounter(9)))("read"),
              _.errorMessage shouldBe expectedMessage,
            )
          } yield inside(error) {
            case CreateSubscriptionError.EventsUnavailable(SequencerCounter(9), message) =>
              message shouldBe expectedMessage
          }
      }

      "not error if there is a counter checkpoint above lower bound" in { env =>
        import env.*

        for {
          _ <- store.registerMember(topologyClientMember, ts0)
          aliceId <- store.registerMember(alice, ts0)
          // write a bunch of events
          delivers = (1L to 20L)
            .map(ts0.plusSeconds)
            .map(Sequenced(_, mockDeliverStoreEvent(sender = aliceId)()))
          _ <- storeAndWatermark(delivers)
          _ <- store
            .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(11), ts(10)))
            .valueOrFail("saveCounterCheckpoint")
          _ <- store.saveLowerBound(ts(10)).valueOrFail("saveLowerBound")
          _ <- reader.read(alice, SequencerCounter(12)).valueOrFail("read")
        } yield succeed // the above not failing is enough of an assertion
      }
    }

    "convert deliver events with too-old signing timestamps" when {

      def setup(env: Env) = {
        import env.*

        for {
          domainParamsO <- cryptoD.headSnapshot.ipsSnapshot.findDynamicDomainParameters()
          domainParams = domainParamsO.valueOrFail("No domain parameters found")
          signingTolerance = domainParams.sequencerSigningTolerance
          signingToleranceInSec = signingTolerance.duration.toSeconds

          _ <- store.registerMember(topologyClientMember, ts0)
          aliceId <- store.registerMember(alice, ts0)
          bobId <- store.registerMember(bob, ts0)

          recipients = NonEmpty(SortedSet, aliceId, bobId)
          testData = Seq(
            // Sequencing ts, signing ts relative to ts0
            (1L, 0L),
            (signingToleranceInSec, 0L),
            (signingToleranceInSec + 1L, 0L),
            (signingToleranceInSec + 2L, 2L),
          )
          batch = Batch.fromClosed(
            testedProtocolVersion,
            ClosedEnvelope(
              ByteString.copyFromUtf8("test envelope"),
              Recipients.cc(alice, bob),
              testedProtocolVersion,
            ),
          )

          delivers = testData.map { case (sequenceTs, signingTs) =>
            val storeEvent = TraceContext
              .withNewTraceContext { eventTraceContext =>
                mockDeliverStoreEvent(
                  sender = aliceId,
                  payloadId = PayloadId(ts0.plusSeconds(sequenceTs)),
                  signingTs = Some(ts0.plusSeconds(signingTs)),
                  traceContext = eventTraceContext,
                )(recipients)
              }
              .map(id => Payload(id, batch.toByteString))
            Sequenced(ts0.plusSeconds(sequenceTs), storeEvent)
          }
          _ <- storePayloadsAndWatermark(delivers)
        } yield (signingTolerance, batch, delivers)
      }

      final case class DeliveredEventToCheck[A](
          delivered: A,
          sequencingTimestamp: CantonTimestamp,
          messageId: MessageId,
          signingTimestamp: CantonTimestamp,
          sequencerCounter: Long,
      )

      def filterForSigningTimestamps[A]
          : PartialFunction[((A, Sequenced[Payload]), Int), DeliveredEventToCheck[A]] = {
        case (
              (
                delivered,
                Sequenced(
                  timestamp,
                  DeliverStoreEvent(
                    _sender,
                    messageId,
                    _members,
                    _payload,
                    Some(signingTimestamp),
                    _traceContext,
                  ),
                ),
              ),
              idx,
            ) =>
          DeliveredEventToCheck(delivered, timestamp, messageId, signingTimestamp, idx.toLong)
      }

      "read by the sender into deliver errors" in { env =>
        import env.*
        setup(env).flatMap { case (signingTolerance, batch, delivers) =>
          for {
            aliceEvents <- readAsSeq(alice, SequencerCounter(0), delivers.length)
          } yield {
            aliceEvents.length shouldBe delivers.length
            aliceEvents.map(_.counter) shouldBe (SequencerCounter(0) until SequencerCounter(
              delivers.length.toLong
            ))
            val deliverWithSigningTimestamps =
              aliceEvents.zip(delivers).zipWithIndex.collect {
                filterForSigningTimestamps
              }
            forEvery(deliverWithSigningTimestamps) {
              case DeliveredEventToCheck(
                    delivered,
                    sequencingTimestamp,
                    messageId,
                    signingTimestamp,
                    sc,
                  ) =>
                val expectedSequencedEvent =
                  if (signingTimestamp + signingTolerance >= sequencingTimestamp)
                    Deliver.create(
                      SequencerCounter(sc),
                      sequencingTimestamp,
                      domainId,
                      messageId.some,
                      batch,
                      testedProtocolVersion,
                    )
                  else
                    DeliverError.create(
                      SequencerCounter(sc),
                      sequencingTimestamp,
                      domainId,
                      messageId,
                      SequencerErrors
                        .SigningTimestampTooEarly(
                          signingTimestamp,
                          sequencingTimestamp,
                        ),
                      testedProtocolVersion,
                    )
                delivered.signedEvent.content shouldBe expectedSequencedEvent
            }
          }
        }
      }

      "read by another recipient into empty batches" in { env =>
        import env.*
        setup(env).flatMap { case (signingTolerance, batch, delivers) =>
          for {
            bobEvents <- readAsSeq(bob, SequencerCounter(0), delivers.length)
          } yield {
            bobEvents.length shouldBe delivers.length
            bobEvents.map(_.counter) shouldBe (0L until delivers.length.toLong)
              .map(SequencerCounter(_))
            val deliverWithSigningTimestamps =
              bobEvents.zip(delivers).zipWithIndex.collect {
                filterForSigningTimestamps
              }
            forEvery(deliverWithSigningTimestamps) {
              case DeliveredEventToCheck(
                    delivered,
                    sequencingTimestamp,
                    _messageId,
                    signingTimestamp,
                    sc,
                  ) =>
                val expectedSequencedEvent =
                  if (signingTimestamp + signingTolerance >= sequencingTimestamp)
                    Deliver.create(
                      SequencerCounter(sc),
                      sequencingTimestamp,
                      domainId,
                      None,
                      batch,
                      testedProtocolVersion,
                    )
                  else
                    Deliver.create(
                      SequencerCounter(sc),
                      sequencingTimestamp,
                      domainId,
                      None,
                      Batch.empty(testedProtocolVersion),
                      testedProtocolVersion,
                    )
                delivered.signedEvent.content shouldBe expectedSequencedEvent
            }
          }
        }
      }

      "do not update the topology client timestamp" in { env =>
        import env.*

        for {
          domainParamsO <- cryptoD.headSnapshot.ipsSnapshot.findDynamicDomainParameters()
          domainParams = domainParamsO.valueOrFail("No domain parameters found")
          signingTolerance = domainParams.sequencerSigningTolerance
          signingToleranceInSec = signingTolerance.duration.toSeconds

          topologyClientMemberId <- store.registerMember(topologyClientMember, ts0)
          aliceId <- store.registerMember(alice, ts0)

          recipientsTopo = NonEmpty(SortedSet, aliceId, topologyClientMemberId)
          recipientsAlice = NonEmpty(SortedSet, aliceId)
          testData = Seq(
            // Sequencing ts, signing ts relative to ts0, recipients
            (1L, None, recipientsTopo),
            (signingToleranceInSec + 1L, Some(0L), recipientsTopo),
          ) ++ (2L to 60L).map(i => (signingToleranceInSec + i, None, recipientsAlice))
          batch = Batch.fromClosed(
            testedProtocolVersion,
            ClosedEnvelope(
              ByteString.copyFromUtf8("test envelope"),
              Recipients.cc(alice, bob),
              testedProtocolVersion,
            ),
          )

          delivers = testData.map { case (sequenceTs, signingTsO, recipients) =>
            val storeEvent = TraceContext
              .withNewTraceContext { eventTraceContext =>
                mockDeliverStoreEvent(
                  sender = aliceId,
                  payloadId = PayloadId(ts0.plusSeconds(sequenceTs)),
                  signingTs = signingTsO.map(ts0.plusSeconds),
                  traceContext = eventTraceContext,
                )(recipients)
              }
              .map(id => Payload(id, batch.toByteString))
            Sequenced(ts0.plusSeconds(sequenceTs), storeEvent)
          }
          _ <- storePayloadsAndWatermark(delivers)
          // take some events
          queue = readWithQueue(alice, SequencerCounter(0))
          // read a bunch of items
          readEvents <- MonadUtil.sequentialTraverse(1L to 61L)(_ => pullFromQueue(queue))
          // wait for a bit over the checkpoint interval (although I would expect because these actions are using the same scheduler the actions may be correctly ordered regardless)
          _ <- waitFor(testConfig.checkpointInterval.underlying * 3)
          // close the queue before we make any assertions
          _ = queue.cancel()
          lastEventRead = readEvents.lastOption.value.value
          _ = logger.debug(s"Fetching checkpoint for event with counter ${lastEventRead.counter}")
          checkpointForLastEventO <- store.fetchClosestCheckpointBefore(
            aliceId,
            lastEventRead.counter + 1,
          )
        } yield {
          // check it created a checkpoint for a recent event
          checkpointForLastEventO.value.counter should be >= SequencerCounter(10)
          checkpointForLastEventO.value.latestTopologyClientTimestamp shouldBe Some(
            // This is before the timestamp of the second event
            CantonTimestamp.ofEpochSecond(1)
          )
        }
      }
    }
  }
}
