// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.OptionT
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.admin.repair.{RepairContext, RepairService}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestData
import com.digitalasset.canton.participant.protocol.{
  MessageCleanReplayStartingPoint,
  MessageProcessingStartingPoint,
  ProcessingStartingPoints,
}
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.MultiDomainEventLog.PublicationData
import com.digitalasset.canton.participant.store.memory.{
  InMemoryMultiDomainEventLog,
  InMemoryOffsetsLookup,
  InMemoryRequestJournalStore,
}
import com.digitalasset.canton.participant.sync.{DefaultLedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.participant.{LocalOffset, RequestOffset}
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, SequencerTestUtils}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.memory.{
  InMemoryIndexedStringStore,
  InMemorySequencedEventStore,
  InMemorySequencerCounterTrackerStore,
}
import com.digitalasset.canton.store.{CursorPrehead, IndexedDomain}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, CloseableTest, RequestCounter, SequencerCounter}
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

class SyncDomainEphemeralStateFactoryTest extends AsyncWordSpec with BaseTest with CloseableTest {

  private lazy val indexedStringStore = InMemoryIndexedStringStore()
  private lazy val domainIdF =
    IndexedDomain.indexed(indexedStringStore)(DomainId.tryFromString("domain::da"))

  private def dummyEvent(
      domainId: DomainId
  )(sc: SequencerCounter, timestamp: CantonTimestamp): OrdinarySerializedEvent =
    OrdinarySequencedEvent(
      SignedContent(
        SequencerTestUtils.mockDeliver(sc.v, timestamp, domainId),
        SymbolicCrypto.emptySignature,
        None,
        testedProtocolVersion,
      ),
      None,
    )(TraceContext.empty)

  private def offsetsLookup = new InMemoryOffsetsLookup {
    override def lookupOffsetsBetween(
        id: EventLogId
    )(fromExclusive: Option[LocalOffset], upToInclusive: Option[LocalOffset])(implicit
        executionContext: ExecutionContext,
        traceContext: TraceContext,
    ): Future[Seq[LocalOffset]] = Future.successful(Seq.empty)
  }

  private def mockMultiDomainEventLog(): MultiDomainEventLog =
    new InMemoryMultiDomainEventLog(
      lookupEvent = _ =>
        (eventLogId, localOffset) =>
          Future.failed(
            new RuntimeException(
              s"Event lookup for $eventLogId at offset $localOffset not implemented"
            )
          ),
      offsetsLookup = offsetsLookup,
      byEventId = _ => _ => OptionT(Future.successful(Option.empty)),
      clock = new SimClock(loggerFactory = loggerFactory),
      metrics = ParticipantTestMetrics,
      transferStoreFor = _ => Left("transferStoreFor is not implemented"),
      indexedStringStore = indexedStringStore,
      timeouts = timeouts,
      futureSupervisor = futureSupervisor,
      loggerFactory = loggerFactory,
    )

  private def dummyTimestampedEvent(localOffset: LocalOffset): TimestampedEvent =
    TimestampedEvent(DefaultLedgerSyncEvent.dummyStateUpdate(), localOffset, None)

  private def requestOffset(rc: RequestCounter): RequestOffset =
    RequestOffset(CantonTimestamp.ofEpochSecond(rc.unwrap), rc)

  "startingPoints" when {
    "there is no clean request" should {
      "return the default" in {
        val rjs = new InMemoryRequestJournalStore(loggerFactory)
        val scts = new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
        val ses = new InMemorySequencedEventStore(loggerFactory)
        val mdel = mockMultiDomainEventLog()

        for {
          domainId <- domainIdF
          startingPoints <- SyncDomainEphemeralStateFactory.startingPoints(
            domainId,
            rjs,
            scts,
            ses,
            mdel,
          )
        } yield {
          startingPoints shouldBe ProcessingStartingPoints.tryCreate(
            MessageCleanReplayStartingPoint.default,
            MessageProcessingStartingPoint.default,
            None,
            None,
          )
        }
      }
    }

    "there is only the clean head request" should {
      "return the clean head" in {
        val rjs = new InMemoryRequestJournalStore(loggerFactory)
        val scts = new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
        val ses = new InMemorySequencedEventStore(loggerFactory)
        val mdel = mockMultiDomainEventLog()
        val rc = RequestCounter(0)
        val sc = SequencerCounter(10)
        val ts = CantonTimestamp.Epoch
        for {
          domainId <- domainIdF
          _ <- rjs.insert(RequestData.clean(rc, ts, ts.plusSeconds(1)))
          _ <- rjs.advancePreheadCleanTo(CursorPrehead(rc, ts))
          _ <- ses.store(Seq(dummyEvent(domainId.item)(sc, ts)))
          withDirtySc <- SyncDomainEphemeralStateFactory.startingPoints(
            domainId,
            rjs,
            scts,
            ses,
            mdel,
          )
          _ <- scts.advancePreheadSequencerCounterTo(CursorPrehead(sc, ts))
          withCleanSc <- SyncDomainEphemeralStateFactory.startingPoints(
            domainId,
            rjs,
            scts,
            ses,
            mdel,
          )
        } yield {
          val cleanReplay = MessageCleanReplayStartingPoint(rc, sc, ts.immediatePredecessor)
          val processing =
            MessageProcessingStartingPoint(Some(requestOffset(rc)), rc + 1L, sc + 1L, ts)

          withDirtySc shouldBe ProcessingStartingPoints.tryCreate(
            cleanReplay,
            processing,
            None,
            None,
          )

          withCleanSc shouldBe ProcessingStartingPoints.tryCreate(
            cleanReplay,
            processing,
            None,
            Some(CursorPrehead(sc, ts)),
          )
        }
      }
    }

    "there are several requests" should {
      "return the right result" in {
        val rjs = new InMemoryRequestJournalStore(loggerFactory)
        val scts = new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
        val ses = new InMemorySequencedEventStore(loggerFactory)
        val mdel = mockMultiDomainEventLog()
        val rc = RequestCounter(0)
        val sc = SequencerCounter(10)
        val ts0 = CantonTimestamp.ofEpochSecond(0)
        val ts1 = CantonTimestamp.ofEpochSecond(1)
        val ts2 = CantonTimestamp.ofEpochSecond(2)
        val ts3 = CantonTimestamp.ofEpochSecond(5)
        val ts4 = CantonTimestamp.ofEpochSecond(7)
        val ts5 = CantonTimestamp.ofEpochSecond(8)
        val ts6 = CantonTimestamp.ofEpochSecond(9)
        for {
          domainId <- domainIdF
          _ <- rjs.insert(RequestData.clean(rc, ts0, ts0.plusSeconds(2)))
          _ <- rjs.insert(RequestData.clean(rc + 1L, ts1, ts1.plusSeconds(1)))
          _ <- rjs.insert(RequestData.clean(rc + 2L, ts2, ts2.plusSeconds(4)))
          _ <- ses.store(
            Seq(
              dummyEvent(domainId.item)(sc, ts0),
              dummyEvent(domainId.item)(sc + 1L, ts1),
              dummyEvent(domainId.item)(sc + 2L, ts2),
              dummyEvent(domainId.item)(sc + 3L, ts3),
              dummyEvent(domainId.item)(sc + 4L, ts4),
              dummyEvent(domainId.item)(sc + 5L, ts5),
              dummyEvent(domainId.item)(sc + 6L, ts6),
            )
          )
          _ <- rjs.advancePreheadCleanTo(CursorPrehead(rc, ts0))
          _ <- scts.advancePreheadSequencerCounterTo(CursorPrehead(sc + 1L, ts1))
          sp1 <- SyncDomainEphemeralStateFactory.startingPoints(domainId, rjs, scts, ses, mdel)
          _ <- rjs.advancePreheadCleanTo(CursorPrehead(rc + 1L, ts1))
          sp2 <- SyncDomainEphemeralStateFactory.startingPoints(domainId, rjs, scts, ses, mdel)
          _ <- rjs.advancePreheadCleanTo(CursorPrehead(rc + 2L, ts2))
          _ <- scts.advancePreheadSequencerCounterTo(CursorPrehead(sc + 3L, ts3))
          sp3 <- SyncDomainEphemeralStateFactory.startingPoints(domainId, rjs, scts, ses, mdel)
          _ <- rjs.insert(RequestData.initial(rc + 4L, ts6))
          sp3a <- SyncDomainEphemeralStateFactory.startingPoints(domainId, rjs, scts, ses, mdel)
          _ <- rjs.insert(RequestData.initial(rc + 3L, ts5))
          sp3b <- SyncDomainEphemeralStateFactory.startingPoints(domainId, rjs, scts, ses, mdel)
          _ <- scts.advancePreheadSequencerCounterTo(CursorPrehead(sc + 6L, ts6))
          sp3c <- SyncDomainEphemeralStateFactory.startingPoints(domainId, rjs, scts, ses, mdel)
        } yield {
          // The clean sequencer counter prehead is ahead of the clean request counter prehead
          sp1.rewoundSequencerCounterPrehead shouldBe Some(CursorPrehead(sc, ts0))
          sp1.cleanReplay shouldBe MessageCleanReplayStartingPoint(rc, sc, ts0.immediatePredecessor)
          sp1.processing shouldBe MessageProcessingStartingPoint(
            Some(requestOffset(rc)),
            rc + 1L,
            sc + 1L,
            ts0,
          )

          // start with request 0 because its commit time is after ts1
          sp2.cleanReplay shouldBe MessageCleanReplayStartingPoint(rc, sc, ts0.immediatePredecessor)
          sp2.processing shouldBe MessageProcessingStartingPoint(
            Some(requestOffset(rc + 1)),
            rc + 2L,
            sc + 2L,
            ts1,
          )
          sp2.rewoundSequencerCounterPrehead shouldBe Some(CursorPrehead(sc + 1L, ts1))

          // replay the latest clean request because the clean sequencer counter prehead is before the commit time
          sp3.cleanReplay shouldBe MessageCleanReplayStartingPoint(
            rc + 2L,
            sc + 2L,
            ts2.immediatePredecessor,
          )
          sp3.processing shouldBe MessageProcessingStartingPoint(
            Some(requestOffset(rc + 2)),
            rc + 3L,
            sc + 4L,
            ts3,
          )
          sp3.rewoundSequencerCounterPrehead shouldBe Some(CursorPrehead(sc + 3L, ts3))

          // we still have to replay the latest clean request
          // because we can't be sure that all subsequent requests have already been inserted into the request journal
          sp3a.cleanReplay shouldBe MessageCleanReplayStartingPoint(
            rc + 2L,
            sc + 2L,
            ts2.immediatePredecessor,
          )
          sp3a.processing shouldBe MessageProcessingStartingPoint(
            Some(requestOffset(rc + 2)),
            rc + 3L,
            sc + 4L,
            ts3,
          )
          sp3a.rewoundSequencerCounterPrehead shouldBe Some(CursorPrehead(sc + 3L, ts3))

          // we don't have to replay the latest clean request
          // if the next request is known to be after the commit time.
          // As the clean sequencer counter prehead is before the commit time, we start with the next dirty sequencer counter.
          sp3b.cleanReplay shouldBe MessageCleanReplayStartingPoint(rc + 3L, sc + 5L, ts4)
          sp3b.processing shouldBe MessageProcessingStartingPoint(
            Some(requestOffset(rc + 2)),
            rc + 3L,
            sc + 5L,
            ts4,
          )
          sp3b.rewoundSequencerCounterPrehead shouldBe Some(CursorPrehead(sc + 3L, ts3))

          // we don't have to replay the latest clean request
          // if the next request is known to be after the commit time.
          // As the clean sequencer counter prehead is after the commit time,
          // we start with the next dirty request and rewind the clean sequencer counter prehead
          sp3c.cleanReplay shouldBe MessageCleanReplayStartingPoint(rc + 3L, sc + 5L, ts4)
          sp3c.processing shouldBe MessageProcessingStartingPoint(
            Some(requestOffset(rc + 2)),
            rc + 3L,
            sc + 5L,
            ts4,
          )
          sp3c.rewoundSequencerCounterPrehead shouldBe Some(CursorPrehead(sc + 4L, ts4))
        }
      }

      "start with the request prehead when the clean sequencer counter prehead lags behind" in {
        val rjs = new InMemoryRequestJournalStore(loggerFactory)
        val scts = new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
        val ses = new InMemorySequencedEventStore(loggerFactory)
        val mdel = mockMultiDomainEventLog()
        val rc = RequestCounter(0)
        val sc = SequencerCounter(10)
        val ts0 = CantonTimestamp.ofEpochSecond(0)
        val ts1 = CantonTimestamp.ofEpochSecond(1)
        val ts2 = CantonTimestamp.ofEpochSecond(2)
        for {
          domainId <- domainIdF
          _ <- rjs.insert(RequestData.clean(rc, ts1, ts2))
          _ <- rjs.insert(
            RequestData.clean(rc + 1L, ts2, ts2, Some(RepairContext.tryCreate("repair request")))
          )
          _ <- ses.store(
            Seq(
              dummyEvent(domainId.item)(sc, ts0),
              dummyEvent(domainId.item)(sc + 1L, ts1),
              dummyEvent(domainId.item)(sc + 2L, ts2),
            )
          )
          _ <- rjs.advancePreheadCleanTo(CursorPrehead(rc + 1L, ts2))
          _ <- scts.advancePreheadSequencerCounterTo(CursorPrehead(sc, ts0))
          sp1 <- SyncDomainEphemeralStateFactory.startingPoints(domainId, rjs, scts, ses, mdel)
        } yield {
          sp1.cleanReplay shouldBe MessageCleanReplayStartingPoint(rc + 2L, sc + 3L, ts2)
          sp1.processing shouldBe MessageProcessingStartingPoint(
            Some(RequestOffset(ts2, rc + 1)),
            rc + 2L,
            sc + 3L,
            ts2,
          )
          sp1.rewoundSequencerCounterPrehead shouldBe Some(CursorPrehead(sc, ts0))
        }
      }
    }

    "the commit times are reversed" should {
      "reprocess the clean request" in {
        val rjs = new InMemoryRequestJournalStore(loggerFactory)
        val scts = new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
        val ses = new InMemorySequencedEventStore(loggerFactory)
        val mdel = mockMultiDomainEventLog()
        val rc = RequestCounter(0)
        val sc = SequencerCounter(10)
        val ts0 = CantonTimestamp.ofEpochSecond(0)
        val ts1 = CantonTimestamp.ofEpochSecond(1)
        val ts2 = CantonTimestamp.ofEpochSecond(2)

        for {
          domainId <- domainIdF
          _ <- rjs.insert(RequestData.clean(rc, ts0, ts0.plusSeconds(5)))
          _ <- rjs.insert(RequestData.clean(rc + 1L, ts1, ts1.plusSeconds(2)))
          _ <- rjs.insert(RequestData.initial(rc + 2L, ts2))
          _ <- ses.store(
            Seq(
              dummyEvent(domainId.item)(sc, ts0),
              dummyEvent(domainId.item)(sc + 1L, ts1),
              dummyEvent(domainId.item)(sc + 3L, ts2),
            )
          )
          _ <- rjs.advancePreheadCleanTo(CursorPrehead(rc, ts0))
          sp0 <- SyncDomainEphemeralStateFactory.startingPoints(domainId, rjs, scts, ses, mdel)
          _ <- rjs.advancePreheadCleanTo(CursorPrehead(rc + 1L, ts1))
          sp2 <- SyncDomainEphemeralStateFactory.startingPoints(domainId, rjs, scts, ses, mdel)
        } yield {
          // start with request 0 because request 1 hasn't yet been marked as clean and request 0 commits after request 1 starts
          sp0.cleanReplay shouldBe MessageCleanReplayStartingPoint(rc, sc, ts0.immediatePredecessor)
          sp0.processing shouldBe MessageProcessingStartingPoint(
            Some(requestOffset(rc)),
            rc + 1L,
            sc + 1L,
            ts0,
          )
          // replay from request 0 because request 2 starts before request 0 commits
          sp2.cleanReplay shouldBe MessageCleanReplayStartingPoint(rc, sc, ts0.immediatePredecessor)
          sp2.processing shouldBe MessageProcessingStartingPoint(
            Some(requestOffset(rc + 1)),
            rc + 2L,
            sc + 3L,
            ts1,
          )
        }
      }
    }

    "there are published events" should {
      "use the latest local offset as the lower bound" in {
        val rjs = new InMemoryRequestJournalStore(loggerFactory)
        val scts = new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
        val ses = new InMemorySequencedEventStore(loggerFactory)
        val mdel = mockMultiDomainEventLog()
        val rc = RequestCounter(0)
        val sc = SequencerCounter(10)
        val ts0 = CantonTimestamp.ofEpochSecond(0)
        val ts1 = CantonTimestamp.ofEpochSecond(1)
        val ts2 = CantonTimestamp.ofEpochSecond(2)

        val firstOffset = requestOffset(rc)
        val secondOffset = firstOffset.focus(_.requestCounter).modify(_ + 2)

        for {
          domainId <- domainIdF
          eventLogId = DomainEventLogId(domainId)
          _ <- mdel.publish(PublicationData(eventLogId, dummyTimestampedEvent(firstOffset), None))
          _ <- mdel.publish(PublicationData(eventLogId, dummyTimestampedEvent(secondOffset), None))
          _ <- Future {
            ()
          } // this is needed to make AsyncWordSpec's serial execution context actually
          _ <- Future {
            ()
          } // do the publication before computing the starting points.
          noCleanReq <- SyncDomainEphemeralStateFactory.startingPoints(
            domainId,
            rjs,
            scts,
            ses,
            mdel,
          )
          _ <- rjs.insert(RequestData.clean(rc + 1L, ts0, ts2))
          _ <- rjs.insert(RequestData.clean(rc + 4L, ts1, ts1))
          _ <- rjs.advancePreheadCleanTo(CursorPrehead(rc + 4L, ts1))
          _ <- ses.store(
            Seq(dummyEvent(domainId.item)(sc, ts0), dummyEvent(domainId.item)(sc + 1L, ts1))
          )
          withCleanReq <- SyncDomainEphemeralStateFactory.startingPoints(
            domainId,
            rjs,
            scts,
            ses,
            mdel,
          )
        } yield {
          noCleanReq.cleanReplay shouldBe MessageCleanReplayStartingPoint.default
          noCleanReq.processing shouldBe MessageProcessingStartingPoint.default
          noCleanReq.lastPublishedRequestOffset shouldBe Some(secondOffset)

          withCleanReq.cleanReplay shouldBe MessageCleanReplayStartingPoint(
            rc + 1L,
            sc,
            ts0.immediatePredecessor,
          )
          withCleanReq.processing shouldBe MessageProcessingStartingPoint(
            Some(RequestOffset(ts1, rc + 4)),
            rc + 5L,
            sc + 2L,
            ts1,
          )
          withCleanReq.lastPublishedRequestOffset shouldBe Some(secondOffset)
        }
      }
    }

    "when there is a dirty repair request" should {
      "not rewind the clean sequencer counter prehead" in {
        val rjs = new InMemoryRequestJournalStore(loggerFactory)
        val scts = new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
        val ses = new InMemorySequencedEventStore(loggerFactory)
        val mdel = mockMultiDomainEventLog()
        val rc = RequestCounter.Genesis
        val sc = SequencerCounter(10)
        val ts0 = CantonTimestamp.ofEpochSecond(0)
        val ts1 = CantonTimestamp.ofEpochSecond(1)

        for {
          domainId <- domainIdF
          _ <- ses.store(
            Seq(dummyEvent(domainId.item)(sc, ts0), dummyEvent(domainId.item)(sc + 1L, ts1))
          )
          _ <- scts.advancePreheadSequencerCounterTo(CursorPrehead(sc, ts0))
          _ <- rjs.insert(
            RequestData.clean(rc + 1L, ts1, ts1, Some(RepairContext.tryCreate("repair1")))
          )
          noCleanRepair <- SyncDomainEphemeralStateFactory.startingPoints(
            domainId,
            rjs,
            scts,
            ses,
            mdel,
          )
          _ <- rjs.insert(RequestData.clean(rc, ts0, ts0, Some(RepairContext.tryCreate("repair0"))))
          _ <- rjs.advancePreheadCleanTo(CursorPrehead(rc, ts0))
          _ <- scts.advancePreheadSequencerCounterTo(CursorPrehead(sc + 1L, ts1))
          withDirtyRepair <- SyncDomainEphemeralStateFactory.startingPoints(
            domainId,
            rjs,
            scts,
            ses,
            mdel,
          )
          _ <- rjs.advancePreheadCleanTo(CursorPrehead(rc + 1L, ts1))
          withCleanRepair <- SyncDomainEphemeralStateFactory.startingPoints(
            domainId,
            rjs,
            scts,
            ses,
            mdel,
          )
        } yield {
          noCleanRepair.cleanReplay shouldBe MessageCleanReplayStartingPoint(
            RequestCounter.Genesis,
            sc + 1L,
            ts0,
          )
          noCleanRepair.processing shouldBe MessageProcessingStartingPoint(
            None,
            RequestCounter.Genesis,
            sc + 1L,
            ts0,
          )
          noCleanRepair.rewoundSequencerCounterPrehead shouldBe Some(CursorPrehead(sc, ts0))

          withDirtyRepair.cleanReplay shouldBe MessageCleanReplayStartingPoint(
            rc + 1L,
            sc + 2L,
            ts1,
          )
          withDirtyRepair.processing shouldBe MessageProcessingStartingPoint(
            Some(requestOffset(rc)),
            rc + 1L,
            sc + 2L,
            ts1,
          )
          withDirtyRepair.rewoundSequencerCounterPrehead shouldBe Some(CursorPrehead(sc + 1L, ts1))

          withCleanRepair.cleanReplay shouldBe MessageCleanReplayStartingPoint(
            rc + 2L,
            sc + 2L,
            ts1,
          )
          withCleanRepair.processing shouldBe MessageProcessingStartingPoint(
            Some(requestOffset(rc + 1)),
            rc + 2L,
            sc + 2L,
            ts1,
          )
          withCleanRepair.rewoundSequencerCounterPrehead shouldBe Some(CursorPrehead(sc + 1L, ts1))
        }
      }
    }

    "there are only repair requests" should {
      "skip over the clean repair requests" in {
        val rjs = new InMemoryRequestJournalStore(loggerFactory)
        val scts = new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
        val ses = new InMemorySequencedEventStore(loggerFactory)
        val mdel = mockMultiDomainEventLog()
        val repairTs = RepairService.RepairTimestampOnEmptyDomain

        for {
          domainId <- domainIdF
          _ <- rjs.insert(
            RequestData.clean(
              RequestCounter.Genesis,
              repairTs,
              repairTs,
              Some(RepairContext.tryCreate("repair0")),
            )
          )
          _ <- rjs.advancePreheadCleanTo(CursorPrehead(RequestCounter.Genesis, repairTs))
          oneRepair <- SyncDomainEphemeralStateFactory.startingPoints(
            domainId,
            rjs,
            scts,
            ses,
            mdel,
          )
          _ <- rjs.insert(
            RequestData.clean(
              RequestCounter.Genesis + 1L,
              repairTs,
              repairTs,
              Some(RepairContext.tryCreate("repair1")),
            )
          )
          _ <- rjs.advancePreheadCleanTo(CursorPrehead(RequestCounter.Genesis + 1L, repairTs))
          twoRepairs <- SyncDomainEphemeralStateFactory.startingPoints(
            domainId,
            rjs,
            scts,
            ses,
            mdel,
          )
          _ <- rjs.insert(
            RequestData
              .clean(
                RequestCounter.Genesis + 2L,
                repairTs,
                repairTs,
                Some(RepairContext.tryCreate("crashed repair")),
              )
          )
          // Repair has crashed before advancing the clean request prehead
          crashedRepair <- SyncDomainEphemeralStateFactory.startingPoints(
            domainId,
            rjs,
            scts,
            ses,
            mdel,
          )
        } yield {
          val startOne = MessageProcessingStartingPoint(
            Some(RequestOffset(repairTs, RequestCounter.Genesis)),
            RequestCounter.Genesis + 1L,
            SequencerCounter.Genesis,
            CantonTimestamp.MinValue,
          )

          oneRepair shouldBe ProcessingStartingPoints.tryCreate(
            startOne.toMessageCleanReplayStartingPoint,
            startOne,
            None,
            None,
          )

          val startTwo = MessageProcessingStartingPoint(
            Some(RequestOffset(repairTs, RequestCounter.Genesis + 1)),
            RequestCounter.Genesis + 2L,
            SequencerCounter.Genesis,
            CantonTimestamp.MinValue,
          )

          twoRepairs shouldBe ProcessingStartingPoints.tryCreate(
            startTwo.toMessageCleanReplayStartingPoint,
            startTwo,
            None,
            None,
          )

          crashedRepair shouldBe twoRepairs
        }
      }
    }
  }
}
