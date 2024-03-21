// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.option.*
import com.daml.lf.data.{ImmArray, Time}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.v2.TransactionMeta
import com.digitalasset.canton.participant.store.db.DbEventLogTestResources
import com.digitalasset.canton.participant.sync.LedgerSyncEvent.PublicPackageUploadRejected
import com.digitalasset.canton.participant.sync.TimestampedEvent.TimelyRejectionEventId
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.participant.{LedgerSyncRecordTime, LocalOffset}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  LedgerSubmissionId,
  LedgerTransactionId,
  LfTimestamp,
  RequestCounter,
  SequencerCounter,
}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.util.UUID
import scala.collection.immutable.HashMap
import scala.concurrent.Future
import scala.language.implicitConversions

trait SingleDimensionEventLogTest extends BeforeAndAfterAll with BaseTest {
  this: AsyncWordSpec =>
  import SingleDimensionEventLogTest.*

  protected lazy val id: EventLogId = DbEventLogTestResources.dbSingleDimensionEventLogEventLogId

  implicit private def localOffset(i: Long): LocalOffset =
    LocalOffset(RequestCounter(i))

  private def generateEventWithTransactionId(
      offset: LocalOffset,
      idString: String,
  ): TimestampedEvent = {

    val transactionMeta =
      TransactionMeta(
        ledgerEffectiveTime = Time.Timestamp.Epoch,
        workflowId = None,
        submissionTime = Time.Timestamp.Epoch,
        submissionSeed = LedgerSyncEvent.noOpSeed,
        optUsedPackages = None,
        optNodeSeeds = None,
        optByKeyNodes = None,
        optDomainId = None,
      )
    val transactionId = LedgerTransactionId.assertFromString(idString)

    val committedTransaction = LfCommittedTransaction(
      LfVersionedTransaction(
        version = LfTransactionVersion.V14,
        nodes = HashMap.empty,
        roots = ImmArray.empty,
      )
    )

    val transactionAccepted = LedgerSyncEvent.TransactionAccepted(
      completionInfoO = None,
      transactionMeta = transactionMeta,
      transaction = committedTransaction,
      transactionId = transactionId,
      recordTime = LfTimestamp.Epoch,
      divulgedContracts = List.empty,
      blindingInfoO = None,
      hostedWitnesses = Nil,
      contractMetadata = Map(), // TODO(#9795) wire proper value
    )

    TimestampedEvent(transactionAccepted, offset, None)
  }

  lazy val domain1: DomainId = DomainId.tryFromString("domain::one")

  def singleDimensionEventLog(mk: () => SingleDimensionEventLog[EventLogId]): Unit = {

    def withEventLog[A](body: SingleDimensionEventLog[EventLogId] => Future[A]): Future[A] = {
      val eventLog = mk()
      for {
        _ <- eventLog.prune(LocalOffset.MaxValue)
        result <- body(eventLog)
      } yield result
    }

    def assertEvents(
        eventLog: SingleDimensionEventLog[EventLogId],
        expectedEvents: Seq[TimestampedEvent],
    ): Future[Assertion] =
      for {
        events <- eventLog.lookupEventRange(None, None, None, None, None)
        lastOffset <- eventLog.lastOffset.value
      } yield {
        val normalizedEvents = events.map { case (offset, event) =>
          offset -> event.normalized
        }.toMap
        normalizedEvents shouldBe expectedEvents
          .map(event => event.localOffset -> event.normalized)
          .toMap
        lastOffset shouldBe expectedEvents.lastOption.map(_.localOffset)
      }

    "empty" should {
      "have an empty last offset and no events" in withEventLog { eventLog =>
        assertEvents(eventLog, Seq.empty)
      }

      "publish some events and read from ledger beginning" in withEventLog { eventLog =>
        val ts = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts, 1L)
        val event2 = generateEvent(ts, 2L)
        logger.debug("Starting 1")
        for {
          _ <- eventLog.insert(event1)
          _ <- assertEvents(eventLog, Seq(event1))
          _ <- eventLog.insert(event2)
          _ <- assertEvents(eventLog, Seq(event1, event2))
        } yield {
          logger.debug("Finished 1")
          succeed
        }
      }

      "support deleteAfter" in withEventLog { eventLog =>
        logger.debug("Starting 3")
        for {
          _ <- eventLog.deleteAfter(2L)
          _ <- assertEvents(eventLog, Seq.empty)
        } yield {
          logger.debug("Finished 3")
          succeed
        }
      }

      "publish an event by ID several times" in withEventLog { eventLog =>
        logger.debug("Starting 4")
        val domainId = DomainId.tryFromString("domain::id")
        val eventId1 = TimelyRejectionEventId(domainId, new UUID(0L, 1L))
        val eventId2 = TimelyRejectionEventId(domainId, new UUID(0L, 2L))

        val event1 =
          generateEventWithTransactionId(1L, "transaction-id").copy(eventId = eventId1.some)
        val event2 =
          generateEventWithTransactionId(2L, "transaction-id").copy(eventId = eventId2.some)
        val event3 = generateEventWithTransactionId(3L, "transaction-id")

        for {
          () <- eventLog.insertUnlessEventIdClash(event1).valueOrFail("First insert")
          () <- eventLog.insertUnlessEventIdClash(event2).valueOrFail("Second insert")
          () <- eventLog.insert(event3)
          _ <- assertEvents(eventLog, Seq(event1, event2, event3))
        } yield {
          logger.debug("Finished 4")
          succeed
        }
      }
    }

    "contains events" should {
      "publish an event and read from the ledger end" in withEventLog { eventLog =>
        logger.debug("Starting 6")
        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1)

        val ts2 = LedgerSyncRecordTime.Epoch.addMicros(5000000L)
        val event2 = generateEvent(ts2, 2)

        for {
          inserts <- eventLog.insertsUnlessEventIdClash(Seq(event1, event2))
          _ <- assertEvents(eventLog, Seq(event1, event2))
        } yield {
          logger.debug("Finished 6")
          inserts shouldBe Seq(Right(()), Right(()))
        }
      }

      "publish events with the same timestamp" in withEventLog { eventLog =>
        logger.debug("Starting 7")
        val ts = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts, 1)
        val event2 = generateEvent(ts, 2)

        for {
          inserts <- eventLog.insertsUnlessEventIdClash(Seq(event1, event2))
          _ <- assertEvents(eventLog, Seq(event1, event2))
        } yield {
          logger.debug("Finished 7")
          inserts shouldBe Seq(Right(()), Right(()))
        }
      }

      "publish events out of order" in withEventLog { eventLog =>
        logger.debug("Starting 8")
        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1)

        val ts2 = LedgerSyncRecordTime.Epoch.addMicros(5000000L)
        val event2 = generateEvent(ts2, 2)

        for {
          _ <- eventLog.insert(event2)
          _ <- eventLog.insert(event1)
          _ <- assertEvents(eventLog, Seq(event1, event2))
        } yield {
          logger.debug("Finished 8")
          succeed
        }
      }

      "be idempotent on duplicate publications" in withEventLog { eventLog =>
        logger.debug("Starting 9")
        val event = generateEvent(LedgerSyncRecordTime.Epoch, 1)

        for {
          _ <- eventLog.insert(event)
          _ <- eventLog.insert(event)
          _ <- assertEvents(eventLog, Seq(event))
        } yield {
          logger.debug("Finished 9")
          succeed
        }
      }

      "publish events with extreme counters and timestamps" in withEventLog { eventLog =>
        logger.debug("Starting 10")
        val event1a =
          generateEvent(
            LedgerSyncRecordTime.MinValue,
            LocalOffset(RequestCounter(0)),
            Some(SequencerCounter(Long.MinValue)),
          )

        val event2 =
          generateEvent(
            LedgerSyncRecordTime.MaxValue,
            Long.MaxValue,
            Some(SequencerCounter.MaxValue),
          )

        val allEvents = Seq(event1a, event2)

        for {
          inserts <- eventLog.insertsUnlessEventIdClash(allEvents)
          _ <- assertEvents(eventLog, allEvents)
        } yield {
          logger.debug("Finished 10")
          inserts shouldBe allEvents.map(_ => Right(()))
        }
      }

      "reject publication of conflicting events" in withEventLog { eventLog =>
        logger.debug("Starting 11")
        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1)

        val ts2 = LedgerSyncRecordTime.Epoch.addMicros(5000000L)
        val event2 = generateEvent(ts2, 1)

        val domainId = DomainId.tryFromString("domain::id")
        val eventId = TimelyRejectionEventId(domainId, new UUID(0L, 1L))

        for {
          _ <- eventLog.insert(event1)
          _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
            eventLog.insert(event2),
            _.getMessage should startWith("Unable to overwrite an existing event. Aborting."),
          )
          _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
            eventLog.insertUnlessEventIdClash(event2.copy(eventId = eventId.some)).value,
            _.getMessage should startWith("Unable to overwrite an existing event. Aborting."),
          )
          _ <- assertEvents(eventLog, Seq(event1))
        } yield {
          logger.debug("Finished 11")
          succeed
        }
      }

      "reject publication of conflicting event ids" in withEventLog { eventLog =>
        logger.debug("Starting 12")
        val domainId = DomainId.tryFromString("domain::id")
        val eventId = TimelyRejectionEventId(domainId, new UUID(0L, 1L))

        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1L).copy(eventId = eventId.some)

        val ts2 = LedgerSyncRecordTime.Epoch.addMicros(5000000L)
        val event2 = generateEvent(ts2, 2L).copy(eventId = eventId.some)

        for {
          () <- eventLog.insertUnlessEventIdClash(event1).valueOrFail("first insert")
          clash <- leftOrFail(eventLog.insertUnlessEventIdClash(event2))("second insert")
          _ <- assertEvents(eventLog, Seq(event1))
          _ <- eventLog.insert(
            event2.copy(eventId = None)
          ) // We can still insert it normally without generating an ID
          _ <- assertEvents(eventLog, Seq(event1, event2.copy(eventId = None)))
        } yield {
          logger.debug("Finished 12")
          clash shouldBe event1
        }
      }

      "reject duplicate transaction ids" in withEventLog { eventLog =>
        logger.debug("Starting 13")
        val event1 = generateEventWithTransactionId(1, "id1")
        val event2 = generateEventWithTransactionId(2, "id2")
        val event3 = generateEventWithTransactionId(3, "id1")

        for {
          _ <- eventLog.insert(event1)
          _ <- eventLog.insert(event2)
          _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
            eventLog.insert(event3),
            _.getMessage shouldBe s"Unable to insert event, as the eventId id ${event1.eventId.value} has already been inserted with offset ${event1.localOffset}.",
          )
        } yield {
          logger.debug("Finished 13")
          succeed
        }
      }

      "lookup events by transaction id" in withEventLog { eventLog =>
        logger.debug("Starting 14")
        val event1 = generateEventWithTransactionId(1, "id1")
        val event2 = generateEventWithTransactionId(2, "id2")
        val event3 = generateEvent(LedgerSyncRecordTime.Epoch, 3)

        for {
          _ <- eventLog.insert(event1)
          _ <- eventLog.insert(event2)
          _ <- eventLog.insert(event3)
          et1 <- eventLog.eventByTransactionId(LedgerTransactionId.assertFromString("id1")).value
          et2 <- eventLog.eventByTransactionId(LedgerTransactionId.assertFromString("id2")).value
          et3 <- eventLog.eventByTransactionId(LedgerTransactionId.assertFromString("id3")).value
        } yield {
          logger.debug("Finished 14")
          et1.value.normalized shouldBe event1.normalized
          et2.value.normalized shouldBe event2.normalized
          et3 shouldBe None
        }
      }
      "correctly prune specified events" in withEventLog { eventLog =>
        logger.debug("Starting 15")
        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1)
        val ts2 = ts1.addMicros(5000000L)
        val event2 = generateEvent(ts2, 2)
        val ts3 = ts2.addMicros(5000000L)
        val event3 = generateEvent(ts3, 3)

        for {
          inserts <- eventLog.insertsUnlessEventIdClash(Seq(event1, event2, event3))
          _ <- eventLog.prune(event2.localOffset)
          _ <- assertEvents(eventLog, Seq(event3))
        } yield {
          logger.debug("Finished 15")
          inserts shouldBe Seq(Right(()), Right(()), Right(()))
        }
      }

      "existsBetween finds time jumps" in withEventLog { eventLog =>
        logger.debug("Starting 16")
        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1L)
        val ts2 = ts1.addMicros(-5000000L)
        val event2 = generateEvent(ts2, 2L)
        val ts3 = ts1.addMicros(5000000L)
        val event3 = generateEvent(ts3, 3L)

        for {
          inserts <- eventLog.insertsUnlessEventIdClash(Seq(event1, event2, event3))
          q1 <- eventLog.existsBetween(CantonTimestamp(ts1), 2L)
          q2 <- eventLog.existsBetween(CantonTimestamp(ts1).immediateSuccessor, 2L)
          q3 <- eventLog.existsBetween(CantonTimestamp(ts2), 0L)
          q4 <- eventLog.existsBetween(
            CantonTimestamp.MaxValue,
            LocalOffset.MaxValue,
          )
          q5 <- eventLog.existsBetween(CantonTimestamp(ts3), 3L)
        } yield {
          logger.debug("Finished 16")
          inserts shouldBe Seq(Right(()), Right(()), Right(()))
          q1 shouldBe true
          q2 shouldBe false
          q3 shouldBe false
          q4 shouldBe false
          q5 shouldBe true
        }
      }

      "delete events from the given request counter" in withEventLog { eventLog =>
        logger.debug("Starting 17")
        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1)
        val ts2 = ts1.addMicros(5000000L)
        val event2 = generateEvent(ts2, 2)
        val ts4 = ts2.addMicros(5000000L)
        val event4 = generateEvent(ts4, 4)
        val ts4a = ts4.addMicros(1L)
        val event4a = generateEvent(ts4a, 4)

        for {
          inserts <- eventLog.insertsUnlessEventIdClash(Seq(event1, event2, event4))
          () <- eventLog.deleteAfter(2L)
          _ <- assertEvents(eventLog, Seq(event1, event2))
          () <- eventLog.insert(event4a) // can insert deleted event with different timestamp
          _ <- assertEvents(eventLog, Seq(event1, event2, event4a))
          () <- eventLog.deleteAfter(1L)
          _ <- assertEvents(eventLog, Seq(event1))
        } yield {
          logger.debug("Finished 17")
          inserts shouldBe Seq(Right(()), Right(()), Right(()))
        }
      }
    }
  }
}

private[participant] object SingleDimensionEventLogTest {
  def generateEvent(
      recordTime: LedgerSyncRecordTime,
      localOffset: LocalOffset,
      requestSequencerCounter: Option[SequencerCounter] = Some(SequencerCounter(42)),
  )(implicit traceContext: TraceContext): TimestampedEvent =
    TimestampedEvent(
      PublicPackageUploadRejected(
        LedgerSubmissionId.assertFromString("submission"),
        recordTime,
        "event",
      ),
      localOffset,
      requestSequencerCounter,
    )
}
