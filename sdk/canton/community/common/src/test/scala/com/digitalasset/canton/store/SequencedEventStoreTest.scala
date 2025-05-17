// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.Validated.Valid
import cats.syntax.parallel.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{Signature, SigningKeyUsage, SigningPublicKey, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.sequencing.{SequencedSerializedEvent, SequencerTestUtils}
import com.digitalasset.canton.store.SequencedEventStore.*
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, CloseableTest, FailOnShutdown, SequencerCounter}
import com.google.protobuf.ByteString
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

trait SequencedEventStoreTest extends PrunableByTimeTest with CloseableTest with FailOnShutdown {
  this: AsyncWordSpec with BaseTest =>

  import com.digitalasset.canton.store.SequencedEventStoreTest.SeqTuple3

  private lazy val crypto: SymbolicCrypto =
    SymbolicCrypto.create(
      testedReleaseProtocolVersion,
      timeouts,
      loggerFactory,
    )

  private lazy val sequencerKey: SigningPublicKey =
    crypto.generateSymbolicSigningKey(usage = SigningKeyUsage.ProtocolOnly)

  def sign(str: String): Signature =
    crypto.sign(TestHash.digest(str), sequencerKey.id, SigningKeyUsage.ProtocolOnly)

  private lazy val synchronizerId: PhysicalSynchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("da::default")
  ).toPhysical

  private def mkBatch(envelopes: ClosedEnvelope*): Batch[ClosedEnvelope] =
    Batch(envelopes.toList, testedProtocolVersion)

  private def signDeliver(event: Deliver[ClosedEnvelope]): SignedContent[Deliver[ClosedEnvelope]] =
    SignedContent(
      event,
      sign(s"deliver signature for ${event.timestamp}"),
      None,
      testedProtocolVersion,
    )

  private lazy val closedEnvelope = ClosedEnvelope.create(
    ByteString.copyFromUtf8("message"),
    RecipientsTest.testInstance,
    Seq.empty,
    testedProtocolVersion,
  )

  private def mkDeliver(ts: CantonTimestamp): SequencedSerializedEvent =
    mkSequencedSerializedEvent(
      SignedContent(
        Deliver.create(
          None,
          ts,
          synchronizerId,
          Some(MessageId.tryCreate("deliver")),
          mkBatch(closedEnvelope),
          None,
          testedProtocolVersion,
          Option.empty[TrafficReceipt],
        ),
        sign("deliver signature"),
        None,
        testedProtocolVersion,
      ),
      nonEmptyTraceContext2,
    )

  private lazy val singleDeliver: SequencedSerializedEvent =
    mkDeliver(CantonTimestamp.ofEpochMilli(-1))

  private lazy val singleMaxDeliverPositive: SequencedSerializedEvent =
    mkSequencedSerializedEvent(
      SignedContent(
        Deliver.create(
          Some(
            CantonTimestamp.MaxValue
          ),
          CantonTimestamp.MaxValue,
          synchronizerId,
          Some(MessageId.tryCreate("single-max-positive-deliver")),
          mkBatch(closedEnvelope),
          Some(CantonTimestamp.MaxValue),
          testedProtocolVersion,
          Option.empty[TrafficReceipt],
        ),
        sign("single deliver signature"),
        None,
        testedProtocolVersion,
      ),
      nonEmptyTraceContext2,
    )

  private val singleMinDeliver: SequencedSerializedEvent =
    mkSequencedSerializedEvent(
      SignedContent(
        Deliver.create(
          None,
          CantonTimestamp.MinValue.immediateSuccessor,
          synchronizerId,
          Some(MessageId.tryCreate("single-min-deliver")),
          mkBatch(closedEnvelope),
          Some(CantonTimestamp.MinValue.immediateSuccessor),
          testedProtocolVersion,
          Option.empty[TrafficReceipt],
        ),
        sign("single deliver signature"),
        None,
        testedProtocolVersion,
      ),
      nonEmptyTraceContext2,
    )

  private def mkDeliverEventTc1(ts: CantonTimestamp): SequencedSerializedEvent =
    mkSequencedSerializedEvent(
      SignedContent(
        SequencerTestUtils.mockDeliver(timestamp = ts, synchronizerId = synchronizerId),
        sign("Mock deliver signature"),
        None,
        testedProtocolVersion,
      ),
      nonEmptyTraceContext1,
    )

  private val event: SequencedSerializedEvent = mkDeliverEventTc1(CantonTimestamp.Epoch)

  private val emptyDeliver: SequencedSerializedEvent =
    mkSequencedSerializedEvent(
      SignedContent(
        Deliver.create(
          None,
          CantonTimestamp.ofEpochMilli(1),
          synchronizerId,
          Some(MessageId.tryCreate("empty-deliver")),
          mkBatch(),
          None,
          testedProtocolVersion,
          Option.empty[TrafficReceipt],
        ),
        sign("Deliver signature"),
        None,
        testedProtocolVersion,
      )
    )

  private def mkDeliverError(ts: CantonTimestamp): SequencedSerializedEvent =
    mkSequencedSerializedEvent(
      SignedContent(
        DeliverError.create(
          Some(
            ts.immediatePredecessor
          ),
          ts,
          synchronizerId,
          MessageId.tryCreate("deliver-error"),
          SequencerErrors.SubmissionRequestRefused("paniertes schnitzel"),
          testedProtocolVersion,
          Option.empty[TrafficReceipt],
        ),
        sign("Deliver error signature"),
        None,
        testedProtocolVersion,
      )
    )

  private def ts(counter: Long): CantonTimestamp = CantonTimestamp.Epoch.addMicros(counter)

  private def mkSequencedSerializedEvent(
      event: SignedContent[SequencedEvent[ClosedEnvelope]],
      traceContext: TraceContext = TraceContext.empty,
  ): SequencedSerializedEvent =
    SequencedEventWithTraceContext(event)(traceContext)

  private def mkEmptyIgnoredEvent(
      counter: Long,
      microsSinceMin: Long = -1,
  ): IgnoredSequencedEvent[Nothing] = {
    val t =
      if (microsSinceMin < 0) ts(counter)
      else CantonTimestamp.MinValue.addMicros(microsSinceMin)
    IgnoredSequencedEvent(t, SequencerCounter(counter), None)(traceContext)
  }

  protected def sequencedEventStore(mkSes: ExecutionContext => SequencedEventStore): Unit = {
    def mk(): SequencedEventStore = mkSes(executionContext)

    behave like prunableByTime(mkSes)

    "not find sequenced events in empty store" in {
      val store = mk()
      val criteria = List(ByTimestamp(CantonTimestamp.Epoch), LatestUpto(CantonTimestamp.MaxValue))
      criteria
        .parTraverse_ { criterion =>
          store
            .find(criterion)
            .value
            .map(res => res shouldBe Left(SequencedEventNotFoundError(criterion)))
        }
        .map(_ => succeed)
    }

    "should find stored sequenced events" in {
      val store = mk()

      val events = List[SequencedSerializedEvent](
        singleDeliver,
        event,
        emptyDeliver,
      )
      val storedEvents = events.zipWithIndex.map { case (event, index) =>
        OrdinarySequencedEvent(
          counter = SequencerCounter(index),
          signedEvent = event.signedEvent,
        )(event.traceContext)
      }
      val criteria = List(
        ByTimestamp(CantonTimestamp.ofEpochMilli(-1)),
        ByTimestamp(CantonTimestamp.Epoch),
        ByTimestamp(CantonTimestamp.ofEpochMilli(1)),
      )

      for {
        _stored <- store.store(events)
        found <- criteria.parTraverse(store.find).toValidatedNec
      } yield {
        assert(found.isValid, "finding deliver events succeeds")
        assert(found.map(_.toSeq) == Valid(storedEvents), "found the right deliver events")
      }
    }

    "store is idempotent" in {
      val store = mk()

      val events1 = List[SequencedSerializedEvent](
        singleDeliver,
        event,
      )
      val events2 = List[SequencedSerializedEvent](
        event,
        emptyDeliver,
      )

      for {
        _ <- store.store(events1).onShutdown(Seq.empty)
        _ <- loggerFactory.assertLogs(
          store.store(events2).onShutdown(Seq.empty),
          _.warningMessage should include(
            "Skipping 1 events with timestamp <= 1970-01-01T00:00:00Z (presumed already processed)"
          ),
        )
      } yield succeed
    }

    "store works for no events" in {
      val store = mk()
      store.store(Seq.empty).map(_ => succeed)
    }

    "find works for many events" in {
      val store = mk()

      val events = (0L to 99L).toList.map { i =>
        mkSequencedSerializedEvent(
          SignedContent(
            SequencerTestUtils
              .mockDeliver(
                timestamp = CantonTimestamp.ofEpochMilli(i * 2),
                synchronizerId = synchronizerId,
              ),
            sign(s"signature $i"),
            None,
            testedProtocolVersion,
          )
        )
      }

      for {
        storedEvents <- store.store(events)
        found <- (0L to 199L).toList
          .parTraverse { i =>
            store.find(ByTimestamp(CantonTimestamp.ofEpochMilli(i))).value
          }
      } yield {
        storedEvents should have size 100L
        storedEvents.zipWithIndex.foreach { case (event, i) =>
          assert(
            event.counter == SequencerCounter(i),
            s"Unexpected counter=${event.counter}, expected: $i",
          )
        }
        assert(found.collect { case Right(ev) => ev.asSequencedSerializedEvent } == events)
        assert(
          found.collect { case Left(error) => error } == (1L to 100L).map(i =>
            SequencedEventNotFoundError(ByTimestamp(CantonTimestamp.ofEpochMilli(2 * i - 1)))
          )
        )
      }
    }

    "get a range by timestamp" in {
      val store = mk()
      val startingCounter = 1000
      val eventCount = 100L
      val firstIndex = 10
      val lastIndex = 90
      val events = (1L to eventCount).toList.map { i =>
        mkSequencedSerializedEvent(
          SignedContent(
            SequencerTestUtils
              .mockDeliver(
                timestamp = CantonTimestamp.Epoch.plusMillis(i * 2),
                synchronizerId = synchronizerId,
              ),
            sign(s"signature $i"),
            None,
            testedProtocolVersion,
          )
        )
      }

      for {
        _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
          SequencerCounter(startingCounter)
        )
        storedEvents <- store.store(events)
        found <- store
          .findRange(
            ByTimestampRange(events(firstIndex).timestamp, events(lastIndex).timestamp),
            None,
          )
          .valueOrFail("")
      } yield {
        storedEvents.zipWithIndex.foreach { case (event, i) =>
          assert(
            event.counter == SequencerCounter(startingCounter + i + 1),
            s"Unexpected counter=${event.counter}, expected: $i",
          )
        }
        assert(
          found.map(_.asSequencedSerializedEvent).toList == events.slice(firstIndex, lastIndex + 1)
        )
      }
    }

    "get a range with a limit" in {
      val store = mk()
      val startingCounter = 1000
      val eventCount = 100L
      val firstIndex = 10
      val limit = 90
      val events = (1L to eventCount).toList.map { i =>
        mkSequencedSerializedEvent(
          SignedContent(
            SequencerTestUtils
              .mockDeliver(
                timestamp = CantonTimestamp.Epoch.plusMillis(i * 2),
                synchronizerId = synchronizerId,
              ),
            sign(s"signature $i"),
            None,
            testedProtocolVersion,
          )
        )
      }

      for {
        _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
          SequencerCounter(startingCounter)
        )
        _ <- store.store(events)
        foundByTs <- store
          .findRange(
            ByTimestampRange(events(firstIndex).timestamp, events.lastOption.value.timestamp),
            Some(limit),
          )
          .valueOrFail("")
      } yield {
        assert(
          foundByTs.map(_.asSequencedSerializedEvent).toList == events.slice(
            firstIndex,
            firstIndex + limit,
          )
        )
      }
    }

    "returns all values within a range when range bounds are not in the store" in {
      val store = mk()
      val startingCounter = 1000
      val eventCount = 100L
      val firstIndex = 10
      val lastIndex = 90
      val delta = 10
      val events = (1L to eventCount).toList.map { i =>
        mkSequencedSerializedEvent(
          SignedContent(
            SequencerTestUtils
              .mockDeliver(
                timestamp = CantonTimestamp.Epoch.plusMillis(i * delta),
                synchronizerId = synchronizerId,
              ),
            sign(s"signature $i"),
            None,
            testedProtocolVersion,
          )
        )
      }

      for {
        _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
          SequencerCounter(startingCounter)
        )
        _ <- store.store(events)
        foundByTs1 <- store
          .findRange(
            ByTimestampRange(
              events(firstIndex).timestamp.minusMillis(delta / 2L),
              events(lastIndex).timestamp.plusMillis(delta / 2L),
            ),
            None,
          )
          .valueOrFail("")
        foundByTs2 <- store
          .findRange(
            ByTimestampRange(
              events.headOption.value.timestamp.minusMillis(delta / 2L),
              events.lastOption.value.timestamp.plusMillis(delta / 2L),
            ),
            None,
          )
          .valueOrFail("")

      } yield {
        assert(
          foundByTs1.map(_.asSequencedSerializedEvent).toList == events.slice(
            firstIndex,
            lastIndex + 1,
          )
        )
        assert(foundByTs2.map(_.asSequencedSerializedEvent).toList == events)
      }
    }

    "find range returns no values for empty store" in {
      val store = mk()
      for {
        foundByTs <- store.findRange(
          ByTimestampRange(CantonTimestamp.Epoch, CantonTimestamp.Epoch.plusMillis(100)),
          None,
        )
      } yield {
        assert(foundByTs.toList == List.empty)
      }
    }.valueOrFail("")

    "find range returns no values when range outside store values" in {
      val store = mk()
      val startingCounter = 149
      val min = 50L
      val max = 100L
      val getTs = { (i: Long) =>
        CantonTimestamp.Epoch.plusMillis(i * 2 + 200)
      }
      val events = (min to max).toList.map { i =>
        mkSequencedSerializedEvent(
          SignedContent(
            SequencerTestUtils
              .mockDeliver(timestamp = getTs(i), synchronizerId = synchronizerId),
            sign(s"signature $i"),
            None,
            testedProtocolVersion,
          )
        )
      }

      for {
        _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
          SequencerCounter(startingCounter)
        )
        _ <- store.store(events)
        foundByTsAbove <- store
          .findRange(ByTimestampRange(getTs(max + 5), getTs(max + 10)), None)
          .valueOrFail("")

        foundByTsBelow <- store
          .findRange(ByTimestampRange(getTs(min - 10), getTs(min - 5)), None)
          .valueOrFail("")

      } yield {
        assert(foundByTsAbove.toList == List.empty)
        assert(foundByTsBelow.toList == List.empty)
      }
    }

    "find range requires that the start of the range is not after the end" in {
      val store = mk()
      val startingCounter = 1000
      val events = (1L to 100L).toList.map { i =>
        mkSequencedSerializedEvent(
          SignedContent(
            SequencerTestUtils
              .mockDeliver(
                timestamp = CantonTimestamp.Epoch.plusMillis(i * 2),
                synchronizerId = synchronizerId,
              ),
            sign(s"signature $i"),
            None,
            testedProtocolVersion,
          )
        )
      }

      for {
        _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
          SequencerCounter(startingCounter)
        )
        _ <- store.store(events)
      } yield {
        assertThrows[IllegalArgumentException](
          store.findRange(
            ByTimestampRange(events.lastOption.value.timestamp, events.headOption.value.timestamp),
            None,
          )
        )
      }
    }

    "find range checks overlap with pruning" in {
      val store = mk()
      val startingCounter = 0
      val events = (1L to 5L).toList.map { i =>
        mkSequencedSerializedEvent(
          SignedContent(
            SequencerTestUtils
              .mockDeliver(
                timestamp = CantonTimestamp.ofEpochSecond(i),
                synchronizerId = synchronizerId,
              ),
            sign(s"signature $i"),
            None,
            testedProtocolVersion,
          )
        )
      }
      val tsPrune = CantonTimestamp.ofEpochSecond(2)
      val ts4 = CantonTimestamp.ofEpochSecond(4)
      val criterionAt = ByTimestampRange(tsPrune, CantonTimestamp.MaxValue)
      val criterionBelow = ByTimestampRange(CantonTimestamp.MinValue, CantonTimestamp.Epoch)
      for {
        _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
          SequencerCounter(startingCounter)
        )
        _ <- store.store(events)
        _ <- store.prune(tsPrune)
        _ <- store
          .findRange(ByTimestampRange(tsPrune.immediateSuccessor, ts4), None)
          .valueOrFail("successful range query")
        fail2 <- leftOrFail(store.findRange(criterionAt, None))("at pruning point")
        failBelow <- leftOrFail(store.findRange(criterionBelow, None))(
          "before pruning point"
        )
      } yield {
        val pruningStatus = PruningStatus(PruningPhase.Completed, tsPrune, Some(tsPrune))
        fail2.criterion shouldBe criterionAt
        fail2.pruningStatus shouldBe pruningStatus
        fail2.foundEvents.map(_.timestamp) shouldBe events
          .filter(_.timestamp > tsPrune)
          .map(_.timestamp)
        failBelow shouldBe SequencedEventRangeOverlapsWithPruning(
          criterionBelow,
          pruningStatus,
          Seq.empty,
        )
      }
    }

    "find returns the latest event" in {
      val store = mk()
      val startingCounter = 99
      val deliverExpectedSc100 =
        mkSequencedSerializedEvent(
          signDeliver(
            SequencerTestUtils
              .mockDeliver(
                timestamp = CantonTimestamp.Epoch,
                synchronizerId = synchronizerId,
              )
          ),
          nonEmptyTraceContext1,
        )
      val deliverExpectedSc101 =
        mkSequencedSerializedEvent(
          signDeliver(
            SequencerTestUtils
              .mockDeliver(
                timestamp = CantonTimestamp.ofEpochSecond(1),
                synchronizerId = synchronizerId,
              )
          ),
          nonEmptyTraceContext2,
        )
      val deliverExpectedSc103 =
        mkSequencedSerializedEvent(
          signDeliver(
            SequencerTestUtils.mockDeliver(
              timestamp = CantonTimestamp.ofEpochSecond(100000),
              synchronizerId = synchronizerId,
            )
          )
        )
      val emptyBatch = mkBatch()
      val deliverExpectedSc102 =
        mkSequencedSerializedEvent(
          signDeliver(
            Deliver.create(
              Some(
                CantonTimestamp.ofEpochSecond(1)
              ),
              CantonTimestamp.ofEpochSecond(2),
              synchronizerId,
              Some(MessageId.tryCreate("deliver1")),
              emptyBatch,
              None,
              testedProtocolVersion,
              Option.empty[TrafficReceipt],
            )
          )
        )
      val deliverExpectedSc104 = mkSequencedSerializedEvent(
        signDeliver(
          Deliver.create(
            Some(
              deliverExpectedSc102.timestamp
            ),
            CantonTimestamp.ofEpochSecond(200000),
            synchronizerId,
            Some(MessageId.tryCreate("deliver2")),
            emptyBatch,
            None,
            testedProtocolVersion,
            Option.empty[TrafficReceipt],
          )
        )
      )

      for {
        _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
          SequencerCounter(startingCounter)
        )
        _ <- store.store(Seq(deliverExpectedSc100))
        findExpectingSc100 <- store
          .find(LatestUpto(CantonTimestamp.MaxValue))
          .valueOrFail("find expecting sc=100")
        _ <- store.store(Seq(deliverExpectedSc101, deliverExpectedSc102, deliverExpectedSc103))
        findExpectingSc103 <- store
          .find(LatestUpto(CantonTimestamp.MaxValue))
          .valueOrFail("find expecting sc=103")
        _ <- store.store(Seq(deliverExpectedSc104))
        findExpectingSc104 <- store
          .find(LatestUpto(deliverExpectedSc104.timestamp))
          .valueOrFail("find expecting sc=104")
        findExpectingSc102 <- store
          .find(LatestUpto(deliverExpectedSc103.timestamp.immediatePredecessor))
          .valueOrFail("find expecting sc=102")
      } yield {
        findExpectingSc100 shouldBe deliverExpectedSc100.asOrdinaryEvent(counter =
          SequencerCounter(100)
        )
        findExpectingSc103 shouldBe deliverExpectedSc103.asOrdinaryEvent(counter =
          SequencerCounter(103)
        )
        findExpectingSc104 shouldBe deliverExpectedSc104.asOrdinaryEvent(counter =
          SequencerCounter(104)
        )
        findExpectingSc102 shouldBe deliverExpectedSc102.asOrdinaryEvent(counter =
          SequencerCounter(102)
        )
      }
    }

    "delete old sequenced events when pruned" in {
      val store = mk()
      val startingCounter = 99

      val ts0 = CantonTimestamp.Epoch
      val ts1 = ts0.plusSeconds(1)
      val ts2 = ts0.plusSeconds(2)
      val ts3 = ts0.plusSeconds(10)
      val ts4 = ts0.plusSeconds(20)

      val firstDeliver =
        mkSequencedSerializedEvent(
          signDeliver(
            SequencerTestUtils.mockDeliver(
              timestamp = ts0,
              synchronizerId = synchronizerId,
            )
          )
        )
      val secondDeliver =
        mkSequencedSerializedEvent(
          signDeliver(
            SequencerTestUtils.mockDeliver(
              timestamp = ts1,
              synchronizerId = synchronizerId,
            )
          )
        )
      val thirdDeliver =
        mkSequencedSerializedEvent(
          signDeliver(
            SequencerTestUtils.mockDeliver(
              timestamp = ts3,
              synchronizerId = synchronizerId,
            )
          )
        )
      val emptyBatch = mkBatch()
      val deliver1 =
        mkSequencedSerializedEvent(
          signDeliver(
            Deliver.create(
              None,
              ts2,
              synchronizerId,
              Some(MessageId.tryCreate("deliver1")),
              emptyBatch,
              None,
              testedProtocolVersion,
              Option.empty[TrafficReceipt],
            )
          )
        )
      val deliver2 =
        mkSequencedSerializedEvent(
          signDeliver(
            Deliver.create(
              Some(
                deliver1.timestamp
              ),
              ts4,
              synchronizerId,
              Some(MessageId.tryCreate("deliver2")),
              emptyBatch,
              None,
              testedProtocolVersion,
              Option.empty[TrafficReceipt],
            )
          )
        )

      for {
        _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
          SequencerCounter(startingCounter)
        )
        _ <- store.store(Seq(firstDeliver, secondDeliver, deliver1, thirdDeliver, deliver2))
        _ <- store.prune(ts2)
        eventsAfterPruningOrPurging <- store.sequencedEvents()
      } yield {
        assert(
          eventsAfterPruningOrPurging.toSet === Set(
            thirdDeliver.asOrdinaryEvent(counter = SequencerCounter(103)),
            deliver2.asOrdinaryEvent(counter = SequencerCounter(104)),
          ),
          "only events with a later timestamp left after pruning",
        )
      }
    }

    "delete all sequenced events when purged" in {
      val store = mk()
      val startingCounter = 99

      val ts0 = CantonTimestamp.Epoch
      val ts1 = ts0.plusSeconds(1)
      val ts2 = ts0.plusSeconds(2)
      val ts3 = ts0.plusSeconds(10)
      val ts4 = ts0.plusSeconds(20)

      val firstDeliver =
        mkSequencedSerializedEvent(
          signDeliver(
            SequencerTestUtils.mockDeliver(
              timestamp = ts0,
              synchronizerId = synchronizerId,
            )
          )
        )
      val secondDeliver =
        mkSequencedSerializedEvent(
          signDeliver(
            SequencerTestUtils.mockDeliver(
              timestamp = ts1,
              synchronizerId = synchronizerId,
            )
          )
        )
      val thirdDeliver =
        mkSequencedSerializedEvent(
          signDeliver(
            SequencerTestUtils.mockDeliver(
              timestamp = ts3,
              synchronizerId = synchronizerId,
            )
          )
        )
      val emptyBatch = mkBatch()
      val deliver1 =
        mkSequencedSerializedEvent(
          signDeliver(
            Deliver.create(
              None,
              ts2,
              synchronizerId,
              Some(MessageId.tryCreate("deliver1")),
              emptyBatch,
              None,
              testedProtocolVersion,
              Option.empty[TrafficReceipt],
            )
          )
        )
      val deliver2 =
        mkSequencedSerializedEvent(
          signDeliver(
            Deliver.create(
              Some(
                deliver1.timestamp
              ),
              ts4,
              synchronizerId,
              Some(MessageId.tryCreate("deliver2")),
              emptyBatch,
              None,
              testedProtocolVersion,
              Option.empty[TrafficReceipt],
            )
          )
        )

      for {
        _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
          SequencerCounter(startingCounter)
        )
        _ <- store.store(Seq(firstDeliver, secondDeliver, deliver1, thirdDeliver, deliver2))
        _ <- store.purge()
        eventsAfterPruningOrPurging <- store.sequencedEvents()
      } yield {
        assert(eventsAfterPruningOrPurging.isEmpty, "no events with left after purging")
      }
    }

    "store events up to Long max limit" in {
      val store = mk()

      val events = List[SequencedSerializedEvent](
        singleMinDeliver,
        event,
        singleMaxDeliverPositive,
      )
      val criteria = List(
        ByTimestamp(CantonTimestamp.MinValue.immediateSuccessor),
        ByTimestamp(CantonTimestamp.Epoch),
        ByTimestamp(CantonTimestamp.MaxValue),
      )

      for {
        _stored <- store.store(events)
        found <- criteria.parTraverse(store.find).toValidatedNec
      } yield {
        assert(found.isValid, "finding deliver events succeeds")
        assert(
          found.map(_.map(_.asSequencedSerializedEvent).toSeq) == Valid(events),
          "found the right deliver events",
        )
      }
    }

    {
      val startingCounter = 9
      lazy val deliver = mkDeliver(ts(10))
      lazy val secondDeliver = mkDeliverEventTc1(ts(11))
      lazy val deliverError = mkDeliverError(ts(12))

      "ignore existing events" in {
        val store = mk()

        for {
          _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
            SequencerCounter(startingCounter)
          )
          eventsWithCounters <- store.store(Seq(deliver, secondDeliver, deliverError))
          (storedDeliver, storedSecondDeliver, storedDeliverError) =
            eventsWithCounters.toTuple3OrFail
          _ <- store.ignoreEvents(SequencerCounter(11), SequencerCounter(11)).valueOrFail("")
          events <- store.sequencedEvents()
          range <- valueOrFail(store.findRange(ByTimestampRange(ts(11), ts(12)), limit = None))(
            "findRange"
          )
          byTimestamp <- valueOrFail(store.find(ByTimestamp(ts(11))))("find by timestamp")
          latestUpTo <- valueOrFail(store.find(LatestUpto(ts(11))))("find latest up to")
        } yield {
          storedDeliver.counter.unwrap shouldBe 10
          storedSecondDeliver.counter.unwrap shouldBe 11
          storedDeliverError.counter.unwrap shouldBe 12
          events shouldBe Seq(storedDeliver, storedSecondDeliver.asIgnoredEvent, storedDeliverError)
          range shouldBe Seq(storedSecondDeliver.asIgnoredEvent, storedDeliverError)
          byTimestamp shouldBe storedSecondDeliver.asIgnoredEvent
          latestUpTo shouldBe storedSecondDeliver.asIgnoredEvent
        }
      }

      "ignore non-existing events" in {
        val store = mk()

        for {
          _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
            SequencerCounter(startingCounter)
          )
          _ <- store.store(Seq(deliver, secondDeliver, deliverError))
          _ <- valueOrFail(store.ignoreEvents(SequencerCounter(13), SequencerCounter(14)))(
            "ignoreEvents"
          )
          events <- store.sequencedEvents()
          range <- valueOrFail(store.findRange(ByTimestampRange(ts(12), ts(14)), limit = None))(
            "findRange"
          )
          ignoredEventByTimestamp <- valueOrFail(store.find(ByTimestamp(ts(13))))(
            "find by timestamp"
          )
          ignoredEventLatestUpTo <- valueOrFail(store.find(LatestUpto(ts(13))))("find latest up to")
        } yield {
          events shouldBe Seq(
            deliver.asOrdinaryEvent(counter = SequencerCounter(10)),
            secondDeliver.asOrdinaryEvent(counter = SequencerCounter(11)),
            deliverError.asOrdinaryEvent(counter = SequencerCounter(12)),
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )
          range shouldBe Seq(
            deliverError.asOrdinaryEvent(counter = SequencerCounter(12)),
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )
          ignoredEventByTimestamp shouldBe mkEmptyIgnoredEvent(13)
          ignoredEventLatestUpTo shouldBe mkEmptyIgnoredEvent(13)
        }
      }

      "ignore existing and non-existing events" in {
        val store = mk()

        for {
          _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
            SequencerCounter(startingCounter)
          )
          eventsWithCounters <- store.store(Seq(deliver, secondDeliver, deliverError))
          (storedDeliver, storedSecondDeliver, storedDeliverError) =
            eventsWithCounters.toTuple3OrFail
          _ <- valueOrFail(store.ignoreEvents(SequencerCounter(11), SequencerCounter(14)))(
            "ignoreEvents"
          )
          events <- store.sequencedEvents()
          range <- valueOrFail(store.findRange(ByTimestampRange(ts(11), ts(13)), limit = None))(
            "findRange"
          )
          deliverByTimestamp <- valueOrFail(store.find(ByTimestamp(ts(10))))("find by timestamp")
          deliverLatestUpTo <- valueOrFail(store.find(LatestUpto(ts(10))))("find latest up to")
        } yield {
          events shouldBe Seq(
            storedDeliver,
            storedSecondDeliver.asIgnoredEvent,
            storedDeliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )
          range shouldBe Seq(
            storedSecondDeliver.asIgnoredEvent,
            storedDeliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
          )
          deliverByTimestamp shouldBe storedDeliver
          deliverLatestUpTo shouldBe storedDeliver
        }
      }

      "add ignored events when empty" in {
        val store = mk()

        for {
          _ <- valueOrFail(store.ignoreEvents(SequencerCounter(10), SequencerCounter(12)))(
            "ignoreEvents"
          )
          events <- store.sequencedEvents()
        } yield {
          events shouldBe Seq(
            mkEmptyIgnoredEvent(10, 1),
            mkEmptyIgnoredEvent(11, 2),
            mkEmptyIgnoredEvent(12, 3),
          )
        }
      }

      "ignore beyond first event" in {
        val store = mk()

        for {
          _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
            SequencerCounter(startingCounter)
          )
          eventsWithCounters <- store.store(Seq(deliver, secondDeliver, deliverError))
          (storedDeliver, storedSecondDeliver, storedDeliverError) =
            eventsWithCounters.toTuple3OrFail
          _ <- valueOrFail(store.ignoreEvents(SequencerCounter(0), SequencerCounter(14)))(
            "ignoreEvents"
          )
          events <- store.sequencedEvents()
        } yield {
          events shouldBe Seq(
            storedDeliver.asIgnoredEvent,
            storedSecondDeliver.asIgnoredEvent,
            storedDeliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )
        }
      }

      "ignore no events" in {
        val store = mk()

        for {
          _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
            SequencerCounter(startingCounter)
          )
          _ <- store.store(Seq(deliver, secondDeliver, deliverError))
          _ <- valueOrFail(store.ignoreEvents(SequencerCounter(1), SequencerCounter(0)))(
            "ignoreEvents1"
          )
          _ <- valueOrFail(store.ignoreEvents(SequencerCounter(11), SequencerCounter(10)))(
            "ignoreEvents2"
          )
          _ <- valueOrFail(store.ignoreEvents(SequencerCounter(21), SequencerCounter(20)))(
            "ignoreEvents3"
          )
          events <- store.sequencedEvents()
        } yield {
          events shouldBe Seq(
            deliver.asOrdinaryEvent(counter = SequencerCounter(10)),
            secondDeliver.asOrdinaryEvent(counter = SequencerCounter(11)),
            deliverError.asOrdinaryEvent(counter = SequencerCounter(12)),
          )
        }
      }

      "ignore ignored events" in {
        val store = mk()

        for {
          _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
            SequencerCounter(startingCounter)
          )
          eventsWithCounters <- store.store(Seq(deliver, secondDeliver, deliverError))
          (storedDeliver, storedSecondDeliver, storedDeliverError) =
            eventsWithCounters.toTuple3OrFail
          _ <- valueOrFail(store.ignoreEvents(SequencerCounter(12), SequencerCounter(13)))(
            "ignoreEvents1"
          )
          _ <- valueOrFail(store.ignoreEvents(SequencerCounter(11), SequencerCounter(14)))(
            "ignoreEvents2"
          )
          events <- store.sequencedEvents()
        } yield {
          events shouldBe Seq(
            storedDeliver,
            storedSecondDeliver.asIgnoredEvent,
            storedDeliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )
        }
      }

      "prevent sequencer counter gaps" in {
        val store = mk()

        for {
          _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
            SequencerCounter(startingCounter)
          )
          _ <- store.store(Seq(deliver, secondDeliver, deliverError))
          err <- store.ignoreEvents(SequencerCounter(20), SequencerCounter(21)).value
          events <- store.sequencedEvents()
        } yield {
          events shouldBe Seq(
            deliver.asOrdinaryEvent(counter = SequencerCounter(10)),
            secondDeliver.asOrdinaryEvent(counter = SequencerCounter(11)),
            deliverError.asOrdinaryEvent(counter = SequencerCounter(12)),
          )
          err shouldBe Left(ChangeWouldResultInGap(SequencerCounter(13), SequencerCounter(19)))
        }
      }

      "unignore events" in {
        val store = mk()

        for {
          _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
            SequencerCounter(startingCounter)
          )
          eventsWithCounters <- store.store(Seq(deliver, secondDeliver, deliverError))
          (storedDeliver, storedSecondDeliver, storedDeliverError) =
            eventsWithCounters.toTuple3OrFail
          _ <- valueOrFail(store.ignoreEvents(SequencerCounter(11), SequencerCounter(14)))(
            "ignoreEvents"
          )

          _ <- valueOrFail(store.unignoreEvents(SequencerCounter(20), SequencerCounter(0)))(
            "unignoreEvents20-0"
          )
          events1 <- store.sequencedEvents()

          _ <- valueOrFail(store.unignoreEvents(SequencerCounter(12), SequencerCounter(12)))(
            "unignoreEvents12"
          )
          events2 <- store.sequencedEvents()

          err3 <- store.unignoreEvents(SequencerCounter(13), SequencerCounter(13)).value
          events3 <- store.sequencedEvents()

          _ <- valueOrFail(store.unignoreEvents(SequencerCounter(14), SequencerCounter(14)))(
            "unignoreEvents14"
          )
          events4 <- store.sequencedEvents()

          _ <- valueOrFail(store.unignoreEvents(SequencerCounter(0), SequencerCounter(20)))(
            "unignoreEvents0-20"
          )
          events5 <- store.sequencedEvents()
        } yield {
          events1 shouldBe Seq(
            storedDeliver,
            storedSecondDeliver.asIgnoredEvent,
            storedDeliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )

          events2 shouldBe Seq(
            storedDeliver,
            storedSecondDeliver.asIgnoredEvent,
            storedDeliverError,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )

          err3 shouldBe Left(ChangeWouldResultInGap(SequencerCounter(13), SequencerCounter(13)))
          events3 shouldBe Seq(
            storedDeliver,
            storedSecondDeliver.asIgnoredEvent,
            storedDeliverError,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )

          events4 shouldBe Seq(
            storedDeliver,
            storedSecondDeliver.asIgnoredEvent,
            storedDeliverError,
            mkEmptyIgnoredEvent(13),
          )

          events5 shouldBe Seq(storedDeliver, storedSecondDeliver, storedDeliverError)
        }
      }

      "delete events" in {
        val store = mk()

        for {
          _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
            SequencerCounter(startingCounter)
          )
          eventsWithCounters <- store.store(Seq(deliver, secondDeliver, deliverError))
          (storedDeliver, storedSecondDeliver, storedDeliverError) =
            eventsWithCounters.toTuple3OrFail
          _ <- valueOrFail(store.ignoreEvents(SequencerCounter(11), SequencerCounter(14)))(
            "ignoreEvents"
          )
          _ <- store.delete(SequencerCounter(15))
          events1 <- store.sequencedEvents()
          _ <- store.delete(SequencerCounter(14))
          events2 <- store.sequencedEvents()
          _ <- store.delete(SequencerCounter(12))
          events3 <- store.sequencedEvents()
          _ <- store.delete(SequencerCounter(0))
          events4 <- store.sequencedEvents()
        } yield {
          events1 shouldBe Seq(
            storedDeliver,
            storedSecondDeliver.asIgnoredEvent,
            storedDeliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )
          events2 shouldBe Seq(
            storedDeliver,
            storedSecondDeliver.asIgnoredEvent,
            storedDeliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
          )
          events3 shouldBe Seq(storedDeliver, storedSecondDeliver.asIgnoredEvent)
          events4 shouldBe Seq.empty
        }
      }
    }

    "store and retrieve trace context" in {
      val store = mk()
      val startingCounter = 0
      val events = List[SequencedSerializedEvent](
        mkDeliver(CantonTimestamp.ofEpochMilli(100)),
        mkDeliverEventTc1(CantonTimestamp.ofEpochMilli(110)),
      )
      for {
        _ <- store.reinitializeFromDbOrSetLowerBound(counterIfEmpty =
          SequencerCounter(startingCounter)
        )
        _ <- store.store(events)
        tc1 <- store.traceContext(CantonTimestamp.ofEpochMilli(100))
        tc2 <- store.traceContext(CantonTimestamp.ofEpochMilli(110))
        tc3 <- store.traceContext(CantonTimestamp.ofEpochMilli(111))
      } yield {
        tc1 shouldBe Some(nonEmptyTraceContext2)
        tc2 shouldBe Some(nonEmptyTraceContext1)
        tc3 shouldBe None
      }
    }

  }
}

object SequencedEventStoreTest {
  private implicit class SeqTuple3[A](val s: Seq[A]) extends AnyVal {
    def toTuple3OrFail: (A, A, A) =
      s match {
        case Seq(a, b, c) => (a, b, c)
        case _ =>
          throw new TestFailedException(
            s"Expected a sequence of 3 elements but got ${s.size} elements: $s",
            0,
          )
      }
  }
}
