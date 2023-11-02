// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink}
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.NegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.participant.event.RecordOrderPublisher.{
  PendingEventPublish,
  PendingTransferPublish,
}
import com.digitalasset.canton.participant.protocol.submission.SequencedSubmission
import com.digitalasset.canton.participant.store.EventLogId.{
  DomainEventLogId,
  ParticipantEventLogId,
}
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.{
  InFlightByMessageId,
  InFlightBySequencingInfo,
  InFlightReference,
}
import com.digitalasset.canton.participant.store.MultiDomainEventLog.{OnPublish, PublicationData}
import com.digitalasset.canton.participant.store.db.DbEventLogTestResources
import com.digitalasset.canton.participant.sync.TimestampedEvent.{
  EventId,
  TimelyRejectionEventId,
  TransactionEventId,
}
import com.digitalasset.canton.participant.sync.{
  DefaultLedgerSyncEvent,
  LedgerSyncEvent,
  TimestampedEvent,
}
import com.digitalasset.canton.participant.{
  GlobalOffset,
  LocalOffset,
  RequestOffset,
  TopologyOffset,
}
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{AkkaUtil, MonadUtil}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{Future, blocking}
import scala.language.implicitConversions

@SuppressWarnings(Array("org.wartremover.warts.Var"))
trait MultiDomainEventLogTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterAll {

  private implicit def toGlobalOffset(i: Long): GlobalOffset = GlobalOffset.tryFromLong(i)

  private implicit def toLocalOffset(i: Long): LocalOffset =
    RequestOffset(CantonTimestamp.ofEpochSecond(i), RequestCounter(i))

  private def toRequestOffset(i: Long): RequestOffset =
    RequestOffset(CantonTimestamp.ofEpochSecond(i), RequestCounter(i))

  protected lazy val indexedStringStore: InMemoryIndexedStringStore =
    DbEventLogTestResources.dbMultiDomainEventLogTestIndexedStringStore

  protected lazy val participantEventLogId: ParticipantEventLogId =
    DbEventLogTestResources.dbMultiDomainEventLogTestParticipantEventLogId

  protected lazy val domainIds: List[DomainId] = for (i <- (0 to 2).toList) yield {
    DomainId.tryFromString(s"MultiDomainEventLogTest::domain$i")
  }

  private lazy val domainEventLogIds: List[DomainEventLogId] =
    domainIds.map(EventLogId.forDomain(indexedStringStore)(_).futureValue)

  private lazy val eventLogIds: List[EventLogId] = domainEventLogIds :+ participantEventLogId

  protected def transferStores: Map[TargetDomainId, TransferStore]

  private def timestampAtOffset(offset: LocalOffset): CantonTimestamp =
    CantonTimestamp.assertFromLong(offset.tieBreaker.abs * 1000)

  private def timestampedEvent(
      eventLogIndex: Int,
      localOffset: LocalOffset,
      maybeEventId: Option[EventId] = None,
  ): TimestampedEvent = {

    val id = s"$eventLogIndex-${localOffset.tieBreaker}"

    TimestampedEvent(
      DefaultLedgerSyncEvent.dummyStateUpdate(timestampAtOffset(localOffset)),
      localOffset,
      None,
      Some(maybeEventId.getOrElse(TransactionEventId(LedgerTransactionId.assertFromString(id)))),
    )
  }

  private lazy val allTestEvents: Seq[(EventLogId, TimestampedEvent, Option[InFlightReference])] =
    Seq(
      // events published in the normal way
      (
        eventLogIds(0),
        timestampedEvent(0, 3),
        InFlightBySequencingInfo(
          domainIds(1),
          SequencedSubmission(SequencerCounter(0), CantonTimestamp.ofEpochSecond(1)),
        ).some,
      ),
      (
        eventLogIds(0),
        timestampedEvent(
          0,
          TopologyOffset.tryCreate(CantonTimestamp.ofEpochMilli(3500), NegativeLong.tryCreate(-1)),
        ),
        None,
      ),
      (
        eventLogIds(1),
        timestampedEvent(1, 5),
        InFlightByMessageId(domainIds(0), MessageId.fromUuid(new UUID(1, 1))).some,
      ),
      (eventLogIds(3), timestampedEvent(3, 1), None),
      (
        eventLogIds(3),
        timestampedEvent(
          eventLogIndex = 3,
          localOffset = 2,
          maybeEventId = Some(
            TimelyRejectionEventId(
              domainId = domainIds(1),
              uuid = new UUID(3, 2),
            )
          ),
        ),
        None,
      ),
      (eventLogIds(1), timestampedEvent(1, 6), None),
      // from here on, events are published through recovery
      (eventLogIds(0), timestampedEvent(0, 4), None),
      (eventLogIds(0), timestampedEvent(0, 5), None),
      (eventLogIds(1), timestampedEvent(1, 7), None),
      // these events are in the SingleDimensionEventLog, but not yet published
      (eventLogIds(1), timestampedEvent(1, 10), None),
      (eventLogIds(3), timestampedEvent(3, 3), None),
    )

  private lazy val recoveryStartIndex = 6
  private lazy val recoveredEventLogEventsCount = 9

  private lazy val outdatedEvent: (EventLogId, TimestampedEvent, Option[InFlightReference]) =
    (
      eventLogIds(0),
      timestampedEvent(0, RequestOffset(CantonTimestamp.MinValue, RequestCounter(0))),
      None,
    )

  private lazy val initialTestEvents
      : Seq[(EventLogId, TimestampedEvent, Option[InFlightReference])] =
    allTestEvents.slice(0, recoveryStartIndex)
  private lazy val initialPublicationTime: CantonTimestamp = CantonTimestamp.ofEpochSecond(10)

  private lazy val lastOffsets
      : Seq[(Map[DomainId, (Option[LocalOffset], Option[RequestOffset])], Option[LocalOffset])] = {
    def offsets(i: Long): (Some[LocalOffset], Some[RequestOffset]) =
      (Some(toLocalOffset(i)), Some(toRequestOffset(i)))

    val noOffsets = (None, None)

    // We want that the first two events of the log have different type of offsets
    val event0LocalOffset = initialTestEvents(0)._2.localOffset.asInstanceOf[RequestOffset]
    val event1LocalOffset = initialTestEvents(1)._2.localOffset.asInstanceOf[TopologyOffset]

    /*
      Expected last local offsets
      For a given log `eventLogId`, there is a change between element i and element (i+1) if the
      i-th event is published in `eventLogId`
     */
    Seq(
      (
        Map(
          domainIds(0) -> (Option(event0LocalOffset), Option(event0LocalOffset)),
          domainIds(1) -> noOffsets,
          domainIds(2) -> noOffsets,
        ),
        None,
      ),
      (
        Map(
          domainIds(0) -> (Option(event1LocalOffset), Option(event0LocalOffset)),
          domainIds(1) -> noOffsets,
          domainIds(2) -> noOffsets,
        ),
        None,
      ),
      (
        Map(
          domainIds(0) -> (Option(event1LocalOffset), Option(event0LocalOffset)),
          domainIds(1) -> offsets(5),
          domainIds(2) -> noOffsets,
        ),
        None,
      ),
      (
        Map(
          domainIds(0) -> (Option(event1LocalOffset), Option(event0LocalOffset)),
          domainIds(1) -> offsets(5),
          domainIds(2) -> noOffsets,
        ),
        Some(1L),
      ),
      (
        Map(
          domainIds(0) -> (Option(event1LocalOffset), Option(event0LocalOffset)),
          domainIds(1) -> offsets(5),
          domainIds(2) -> noOffsets,
        ),
        Some(2L),
      ),
      (
        Map(
          domainIds(0) -> (Option(event1LocalOffset), Option(event0LocalOffset)),
          domainIds(1) -> offsets(6),
          domainIds(2) -> noOffsets,
        ),
        Some(2L),
      ),
    )
  }

  private lazy val recoveryBounds: Seq[(EventLogId, Option[LocalOffset])] = Seq(
    eventLogIds(0) -> None, // recover all events
    eventLogIds(1) -> Some(8), // recover some events
    eventLogIds(2) -> Some(Long.MaxValue), // recover no events, as there is no event
    eventLogIds(3) -> Some(0), // recover no events, as the bound is in the past
  )
  private lazy val recoveryPublicationTime: CantonTimestamp = CantonTimestamp.ofEpochSecond(30)

  private lazy val testEventsForRecoveredEventLog
      : Seq[(EventLogId, TimestampedEvent, Option[InFlightReference])] =
    allTestEvents.slice(0, recoveredEventLogEventsCount)

  private lazy val publishedThroughRecovery
      : Seq[(EventLogId, TimestampedEvent, Option[InFlightReference])] =
    allTestEvents.slice(recoveryStartIndex, recoveredEventLogEventsCount)

  private lazy val numPrunedEvents = 5
  private lazy val testEventsForPrunedEventLog
      : Seq[(EventLogId, TimestampedEvent, Option[InFlightReference])] =
    allTestEvents.slice(numPrunedEvents, recoveredEventLogEventsCount)

  protected def storeEventsToSingleDimensionEventLogs(
      events: Seq[(EventLogId, TimestampedEvent)]
  ): Future[Unit]

  private lazy val lockActorSystem = new Object()
  private var actorSystemVar: Option[ActorSystem] = None

  implicit def actorSystem: ActorSystem = getOrCreateActorSystem()

  private def getOrCreateActorSystem(): ActorSystem = blocking {
    lockActorSystem.synchronized {
      actorSystemVar.getOrElse {
        val newActorySystem = AkkaUtil.createActorSystem(loggerFactory.threadName)(executorService)
        actorSystemVar = Some(newActorySystem)
        newActorySystem
      }
    }
  }

  private lazy val lockEventLog = new Object()
  private var eventLogVar: Option[MultiDomainEventLog] = None

  private var globalOffsets: Seq[GlobalOffset] = Seq.empty

  // If necessary, clean up the stores before the test starts
  protected def cleanUpEventLogs(): Unit

  override def beforeAll(): Unit = {
    super.beforeAll()
    globalOffsets = Seq.empty // Reset global offsets
    cleanUpEventLogs()
    storeEventsToSingleDimensionEventLogs(allTestEvents.map {
      case (eventLogId, event, _inFlightRef) => eventLogId -> event
    }).futureValue
  }

  override def afterAll(): Unit = {
    cleanUpEventLogs()
    val eventLogClose: AutoCloseable = () => {
      eventLogVar.foreach { log =>
        eventLogVar = None
        log.close()
      }
    }
    val actorSystemClose: AutoCloseable = () => {
      actorSystemVar.foreach { as =>
        actorSystemVar = None
        Lifecycle.toCloseableActorSystem(as, logger, timeouts)
      }
    }
    Lifecycle.close(eventLogClose, actorSystemClose)(logger)
    super.afterAll()
  }

  private[store] def multiDomainEventLog(mk: Clock => MultiDomainEventLog): Unit = {
    val clock = new SimClock(loggerFactory = loggerFactory)

    def eventLog: MultiDomainEventLog = blocking {
      lockEventLog.synchronized {
        eventLogVar.getOrElse {
          val newEventLog = mk(clock)
          eventLogVar = Some(newEventLog)
          newEventLog
        }
      }
    }

    def eventsFromSubscription(
        beginWith: Option[GlobalOffset]
    ): Future[Seq[(GlobalOffset, Traced[LedgerSyncEvent])]] = {
      val flow = eventLog
        .subscribe(beginWith)
        .takeWithin {
          1.second // generous timeout to avoid flaky test failures
        }
        .toMat(Sink.seq)(Keep.right)
      AkkaUtil.runSupervised(throw _, flow)
    }

    def publishEvents(
        events: Seq[(EventLogId, TimestampedEvent, Option[InFlightReference])]
    ): Unit = {
      MonadUtil
        .sequentialTraverse_(events) { case (id, event, reference) =>
          eventLog.publish(PublicationData(id, event, reference))
        }
        .futureValue
    }

    def expectPublication(
        events: Seq[(EventLogId, TimestampedEvent, Option[InFlightReference])]
    ): OnPublishListener = {
      val eventReferences = events.map { case (_eventLogId, _event, reference) =>
        reference
      }
      val listener = new OnPublishListener(eventReferences)
      eventLog.setOnPublish(listener)
      listener
    }

    class OnPublishListener(expectedReferences: Seq[Option[InFlightReference]])
        extends MultiDomainEventLog.OnPublish {
      private val outstanding
          : AtomicReference[Either[RuntimeException, Seq[Option[InFlightReference]]]] =
        new AtomicReference(Right(expectedReferences))
      override def notify(
          published: Seq[OnPublish.Publication]
      )(implicit batchTraceContext: TraceContext): Unit = {
        val eventReferences = published.map(_.inFlightReferenceO)
        outstanding.getAndUpdate {
          case Right(old) =>
            if (old.startsWith(eventReferences)) {
              Right(old.drop(published.size))
            } else
              Left(
                new RuntimeException(
                  show"OnPublishListener did not get expected event references.\nExpected: $old\nActual: $eventReferences\nPublications: $published"
                )
              )
          case err @ Left(_) => err
        }.discard
      }

      def checkAllNotified: Assertion = {
        val outcome = outstanding.get().valueOr(err => throw err)
        outcome shouldBe Seq.empty
      }
    }

    def subscribeAndCheckEvents(
        beginWith: Option[GlobalOffset],
        expectedTimestampedEvents: Seq[(EventLogId, TimestampedEvent, Option[InFlightReference])],
    ): Future[Assertion] =
      for {
        storedEventsWithOffsets <- eventsFromSubscription(beginWith)
      } yield {
        val (storedOffsets, storedEvents) = storedEventsWithOffsets.unzip

        val expectedEvents = expectedTimestampedEvents.map { case (_, timestampedEvent, _) =>
          Traced(timestampedEvent.event)(timestampedEvent.traceContext)
        }
        storedEvents shouldBe expectedEvents

        storedOffsets.toSet should have size storedOffsets.size.toLong
      }

    def lookupEventRangeAndCheckEvents(
        upToInclusive: Option[GlobalOffset],
        expectedTimestampedEvents: Seq[(EventLogId, TimestampedEvent, Option[InFlightReference])],
    ): Unit = {
      val storedOffsetWithEvents = eventLog.lookupEventRange(upToInclusive, None).futureValue

      val (storedOffsets, storedEvents) = storedOffsetWithEvents.unzip
      val expectedEvents = expectedTimestampedEvents.map { case (_, timestampedEvent, _) =>
        timestampedEvent
      }
      storedEvents shouldBe expectedEvents

      storedOffsets.toSet should have size storedOffsets.size.toLong

      val firstOffset = eventLog.locateOffset(0).value.futureValue
      firstOffset shouldBe storedOffsets.headOption
      val lastOffset = eventLog.locateOffset(storedOffsets.size.toLong - 1L).value.futureValue
      lastOffset shouldBe storedOffsets.lastOption
    }

    def checkOffsetByTime(
        upToInclusive: CantonTimestamp,
        expectedOffsetUpToO: Option[GlobalOffset],
        expectedOffsetAtOrAfterO: Option[(GlobalOffset, EventLogId, LocalOffset)],
    ): Assertion = {
      val globalOffsetUpToO = eventLog.getOffsetByTimeUpTo(upToInclusive).value.futureValue
      globalOffsetUpToO shouldBe expectedOffsetUpToO
      val globalOffsetAtOrAfterO =
        eventLog.getOffsetByTimeAtOrAfter(upToInclusive).value.futureValue
      globalOffsetAtOrAfterO shouldBe expectedOffsetAtOrAfterO
    }

    def updateGlobalOffsets(expectedSize: Int, eventsPruned: Int = 0): Unit = {
      val newOffsets = eventsFromSubscription(None).futureValue.map { case (offset, _) => offset }

      newOffsets should have size expectedSize.toLong
      globalOffsets.drop(eventsPruned) shouldBe newOffsets.take(globalOffsets.size - eventsPruned)
      globalOffsets = newOffsets
    }

    def checkEventLookupByIdForInitialEvents(
        expected: Seq[(EventLogId, TimestampedEvent, Option[InFlightReference])]
    ): Assertion = {
      val initialEventIds = initialTestEvents.mapFilter { case (_eventLogId, event, _inFlightRef) =>
        event.eventId
      }
      val expectedResult = expected.zipWithIndex.mapFilter {
        case ((_eventLogId, event, _inFlightRefO), index) =>
          event.eventId.map(eventId =>
            eventId -> (globalOffsets(index), event, initialPublicationTime)
          )
      }.toMap

      eventLog.lookupByEventIds(initialEventIds).futureValue shouldBe expectedResult
    }

    "A MultiDomainEventLog" when {
      "empty" should {
        lazy val optionalBounds: Seq[Option[GlobalOffset]] = Seq(
          None,
          Some(42),
          Some(MultiDomainEventLog.ledgerFirstOffset),
          Some(Long.MaxValue),
        )
        lazy val bounds = optionalBounds.collect { case Some(x) => x }

        "return no events through subscription" in {
          val tests = optionalBounds.collect {
            case beginWith if beginWith.forall(_ >= MultiDomainEventLog.ledgerFirstOffset) =>
              beginWith -> eventsFromSubscription(beginWith)
          }

          forEvery(tests) { case (beginWith, eventsF) =>
            withClue(s"beginWith = $beginWith") {
              eventsF.futureValue shouldBe empty
            }
          }
        }

        "return no events through range query" in {
          forEvery(optionalBounds) { upToInclusive =>
            eventLog.lookupEventRange(upToInclusive, None).futureValue shouldBe empty
          }
        }

        "find no events by event ID" in {
          checkEventLookupByIdForInitialEvents(Seq.empty)
        }

        "yield empty domain offsets" in {
          forEvery(bounds) { upToInclusive =>
            val emptyDomainOffsets = domainIds.map(_ -> (None, None)).toMap

            eventLog
              .lastDomainOffsetsBeforeOrAtGlobalOffset(
                upToInclusive,
                List.empty,
                participantEventLogId,
              )
              .futureValue shouldBe ((Map.empty, None))

            eventLog
              .lastDomainOffsetsBeforeOrAtGlobalOffset(
                upToInclusive,
                domainIds,
                participantEventLogId,
              )
              .futureValue shouldBe (emptyDomainOffsets, None)
          }
        }

        "yield empty last local/request offsets" in {
          forEvery(bounds) { upToInclusive =>
            val upToInclusiveO = Option(upToInclusive)

            // Participant event log
            eventLog
              .lastLocalOffset(participantEventLogId, upToInclusiveO, None)
              .futureValue shouldBe None

            eventLog
              .lastRequestOffset(participantEventLogId, upToInclusiveO)
              .futureValue shouldBe None

            // Domain event logs
            forEvery(domainEventLogIds) { domainId =>
              eventLog
                .lastLocalOffset(domainId, upToInclusiveO, None)
                .futureValue shouldBe None

              eventLog
                .lastRequestOffset(domainId, upToInclusiveO)
                .futureValue shouldBe None
            }
          }
        }

        "no last local offsets are known" in {
          forEvery(eventLogIds) { eventLogId =>
            eventLog.lastLocalOffset(eventLogId).futureValue shouldBe None
          }
        }

        "no global offset is known" in {
          eventLog.lastGlobalOffset().value.futureValue shouldBe None
        }

        "no offset can be located by delta from beginning" in {
          eventLog.locateOffset(0).value.futureValue shouldBe None
        }

        "no offset can be located by timestamp" in {
          checkOffsetByTime(CantonTimestamp.MaxValue, None, None)
        }

        "no local offsets have been published" in {
          eventLog.globalOffsetFor(participantEventLogId, 1L).futureValue shouldBe None
        }

        "no global offsets can be looked up" in {
          eventLog.lookupOffset(5L).value.futureValue shouldBe None
        }

        "lower bound on publication times is MinValue" in {
          eventLog.publicationTimeLowerBound shouldBe CantonTimestamp.MinValue
        }

        "allow for publishing new events" in {
          // Set the publication time for the events
          clock.advanceTo(initialPublicationTime)
          val listener = expectPublication(initialTestEvents)
          publishEvents(initialTestEvents)
          eventually() {
            listener.checkAllNotified
          }
        }
      }

      "non-empty" should {
        "yield correct events through subscription" in {
          updateGlobalOffsets(initialTestEvents.size)

          val tests = List(
            "none" -> subscribeAndCheckEvents(None, initialTestEvents),
            "ledger first offset" -> subscribeAndCheckEvents(
              Some(MultiDomainEventLog.ledgerFirstOffset),
              initialTestEvents,
            ),
            "after last offset" -> subscribeAndCheckEvents(
              globalOffsets.lastOption.map(_.increment),
              Seq.empty,
            ),
            "max value" -> subscribeAndCheckEvents(Some(Long.MaxValue), Seq.empty),
          ) ++ globalOffsets.zipWithIndex.map { case (beginWith, index) =>
            s"starting at $beginWith" -> subscribeAndCheckEvents(
              Some(beginWith),
              initialTestEvents.drop(index),
            )
          }

          forEvery(tests) { case (hint, assertionF) =>
            withClue(hint) {
              assertionF.futureValue
            }
          }
        }

        "yield correct events through range query" in {
          forEvery(globalOffsets.zipWithIndex) { case (upToInclusive, index) =>
            lookupEventRangeAndCheckEvents(Some(upToInclusive), initialTestEvents.take(index + 1))
          }

          lookupEventRangeAndCheckEvents(None, initialTestEvents)
          lookupEventRangeAndCheckEvents(Some(Long.MaxValue), initialTestEvents)
        }

        "yield correct lastDomainOffsetsBeforeOrAtGlobalOffset" in {
          forEvery(globalOffsets.zipWithIndex) { case (upToInclusive, index) =>
            val (expectedDomainOffsets, expectedParticipantOffset) = lastOffsets(index)

            eventLog
              .lastDomainOffsetsBeforeOrAtGlobalOffset(
                upToInclusive,
                domainIds,
                participantEventLogId,
              )
              .futureValue shouldBe (expectedDomainOffsets, expectedParticipantOffset)

            eventLog
              .lastDomainOffsetsBeforeOrAtGlobalOffset(
                upToInclusive,
                List.empty,
                participantEventLogId,
              )
              .futureValue shouldBe ((Map.empty, expectedParticipantOffset))
          }

          eventLog
            .lastDomainOffsetsBeforeOrAtGlobalOffset(
              Long.MaxValue,
              domainIds,
              participantEventLogId,
            )
            .futureValue shouldBe lastOffsets.lastOption.value

          val (lastDomainOffsets, lastParticipantOffset) = lastOffsets.lastOption.value
          val lastOffsetOfFirstDomain = lastDomainOffsets.filter { case (domainId, _) =>
            domainId == domainIds(0)
          }
          eventLog
            .lastDomainOffsetsBeforeOrAtGlobalOffset(
              Long.MaxValue,
              domainIds.take(1),
              participantEventLogId,
            )
            .futureValue shouldBe
            ((lastOffsetOfFirstDomain, lastParticipantOffset))
        }

        "yield correct lastLocalOffset" in {
          val (lastDomainOffsets, _) = lastOffsets.lastOption.value

          forEvery(eventLogIds.collect { case eventLogId @ DomainEventLogId(domainId) =>
            eventLogId -> domainId
          }) { case (eventLogId, domainId) =>
            val expectedOffset =
              lastDomainOffsets.get(domainId.item).flatMap { case (localOffset, _requestOffset) =>
                localOffset // call to eventLog.lastLocalOffset only returns localOffset
              }

            eventLog.lastLocalOffset(eventLogId).futureValue shouldBe expectedOffset
          }
        }

        "yield correct last local/request offsets" in {
          forEvery(globalOffsets.zipWithIndex) { case (upToInclusive, index) =>
            val upToInclusiveO = Option(upToInclusive)
            val (expectedDomainOffsets, expectedParticipantOffset) = lastOffsets(index)

            forEvery(domainEventLogIds) { domainId =>
              val (expectedLocalOffset, expectedRequestOffset) =
                expectedDomainOffsets.get(domainId.domainId).value

              eventLog
                .lastLocalOffset(domainId, upToInclusiveO, None)
                .futureValue shouldBe expectedLocalOffset

              eventLog
                .lastRequestOffset(domainId, upToInclusiveO, None)
                .futureValue shouldBe expectedRequestOffset
            }

            eventLog
              .lastLocalOffset(participantEventLogId, upToInclusiveO, None)
              .futureValue shouldBe expectedParticipantOffset

            eventLog
              .lastRequestOffset(participantEventLogId, upToInclusiveO, None)
              .futureValue shouldBe expectedParticipantOffset
          }
        }

        "yield correct last global offsets" in {
          eventLog.lastGlobalOffset().value.futureValue shouldBe globalOffsets.lastOption
          forEvery(globalOffsets) { globalOffset =>
            eventLog.lastGlobalOffset(Some(globalOffset)).value.futureValue shouldBe Some(
              globalOffset
            )
          }
        }

        "convert timestamps into offset according to publication time" in {
          val firstEvent =
            (globalOffsets(0), initialTestEvents(0)._1, initialTestEvents(0)._2.localOffset)
          checkOffsetByTime(
            initialPublicationTime.immediatePredecessor,
            None,
            firstEvent.some,
          )
          checkOffsetByTime(initialPublicationTime, globalOffsets.lastOption, firstEvent.some)
          checkOffsetByTime(
            initialPublicationTime.immediateSuccessor,
            globalOffsets.lastOption,
            None,
          )
        }

        "advance the lower bound on publication times" in {
          eventLog.publicationTimeLowerBound shouldBe initialPublicationTime
        }

        "deduplicate repeated publication" in {
          clock.advanceTo(initialPublicationTime.plusSeconds(10))
          val lastOffset = globalOffsets.lastOption

          val listener = expectPublication(Seq.empty)
          publishEvents(initialTestEvents)
          lookupEventRangeAndCheckEvents(None, initialTestEvents)
          listener.checkAllNotified

          globalOffsets.lastOption shouldBe lastOffset
          checkOffsetByTime(
            initialPublicationTime,
            lastOffset,
            (globalOffsets(0), initialTestEvents(0)._1, initialTestEvents(0)._2.localOffset).some,
          ) // Republication does not update the publication time
          eventLog.publicationTimeLowerBound shouldBe initialPublicationTime
        }

        "locate published offsets" in {
          forEvery(initialTestEvents.zipWithIndex) { case ((eventLogId, event, _reference), i) =>
            eventLog.globalOffsetFor(eventLogId, event.localOffset).futureValue shouldBe
              Some(globalOffsets(i) -> initialPublicationTime)
          }
        }

        "lookup published offsets" in {
          forEvery(initialTestEvents.zipWithIndex) { case ((eventLogId, event, _reference), i) =>
            eventLog.lookupOffset(globalOffsets(i)).value.futureValue shouldBe ((
              eventLogId,
              event.localOffset,
              initialPublicationTime,
            ).some)
          }
        }

        "lookup events by ID" in {
          checkEventLookupByIdForInitialEvents(initialTestEvents)
        }

        "allow for recovery" in {
          clock.advanceTo(recoveryPublicationTime)
          forEvery(recoveryBounds.zipWithIndex) { case ((id, upToInclusive), index) =>
            if (index == 1) {
              clock.reset() // reset the clock before recovering the second set of events
            }
            val unpublished = eventLog.fetchUnpublished(id, upToInclusive).futureValue

            val expectedUnpublished = publishedThroughRecovery
              .collect {
                case (eventLogId, event, _ifr) if id == eventLogId =>
                  PublicationData(eventLogId, event, None)
              }
            val unpublishedEvents = unpublished.map {
              case PendingEventPublish(event, _ts, eventLogId) =>
                PublicationData(eventLogId, event, None)
              case PendingTransferPublish(_ts, _eventLogId) =>
                fail("Cannot publish a transfer")
            }
            unpublishedEvents shouldBe expectedUnpublished

            unpublishedEvents.foreach(p => eventLog.publish(p).futureValue)
            eventLog.flush().futureValue
          }
        }
      }

      "recovered" should {

        "contain correct events" in {
          updateGlobalOffsets(testEventsForRecoveredEventLog.size)
          lookupEventRangeAndCheckEvents(None, testEventsForRecoveredEventLog)
        }

        "advance the publication time bound" in {
          eventLog.publicationTimeLowerBound shouldBe recoveryPublicationTime
        }

        "locate correct offset by timestamp" in {
          val index = initialTestEvents.size
          val firstEvent = (globalOffsets(0), allTestEvents(0)._1, allTestEvents(0)._2.localOffset)
          val firstRecoveredEvent = (
            globalOffsets(index),
            allTestEvents(index)._1,
            allTestEvents(index)._2.localOffset,
          )
          checkOffsetByTime(
            recoveryPublicationTime,
            globalOffsets.lastOption,
            firstRecoveredEvent.some,
          )
          checkOffsetByTime(
            recoveryPublicationTime.immediateSuccessor,
            globalOffsets.lastOption,
            None,
          )
          checkOffsetByTime(
            initialPublicationTime,
            globalOffsets(index - 1).some,
            firstEvent.some,
          )
          checkOffsetByTime(
            CantonTimestamp.Epoch,
            None,
            firstEvent.some,
          )
        }

        "allow for pruning" in {
          eventLog.prune(globalOffsets(numPrunedEvents - 1)).futureValue
        }
      }

      "pruned" should {
        "contain correct events" in {
          updateGlobalOffsets(testEventsForPrunedEventLog.size, eventsPruned = numPrunedEvents)
          lookupEventRangeAndCheckEvents(None, testEventsForPrunedEventLog)
          val index = initialTestEvents.size - numPrunedEvents
          val firstRecoveredEvent = (
            globalOffsets(index),
            allTestEvents(index + numPrunedEvents)._1,
            allTestEvents(index + numPrunedEvents)._2.localOffset,
          )
          checkOffsetByTime(
            recoveryPublicationTime,
            globalOffsets.lastOption,
            firstRecoveredEvent.some,
          )
          checkOffsetByTime(
            recoveryPublicationTime.immediateSuccessor,
            globalOffsets.lastOption,
            None,
          )
          checkOffsetByTime(
            initialPublicationTime,
            globalOffsets(index - 1).some,
            (
              globalOffsets(index - 1),
              allTestEvents(index + numPrunedEvents - 1)._1,
              allTestEvents(index + numPrunedEvents - 1)._2.localOffset,
            ).some,
          )
        }

        // Run this test at the end, because it make the event log unusable
        "reject publication of old events" in {
          def checkException: Throwable => Assertion =
            _.getMessage shouldBe s"Unable to publish event at id ${domainIds(0)} and localOffset ${outdatedEvent._2.localOffset}, as that would reorder events."

          val listener = expectPublication(Seq.empty)
          loggerFactory.assertLogs(
            {
              publishEvents(Seq(outdatedEvent))
              eventually() {
                loggerFactory.numberOfRecordedEntries should be >= 2
              }
            },
            e1 => {
              e1.errorMessage shouldBe "An internal error has occurred."
              checkException(e1.throwable.value)
            },
            e2 => {
              e2.errorMessage shouldBe "An exception occurred while publishing an event. Stop publishing events."
              checkException(e2.throwable.value)
            },
          )

          lookupEventRangeAndCheckEvents(None, testEventsForPrunedEventLog)
          listener.checkAllNotified
        }
      }
    }
  }
}
