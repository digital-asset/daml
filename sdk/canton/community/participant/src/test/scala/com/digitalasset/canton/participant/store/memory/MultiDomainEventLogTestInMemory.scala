// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.OptionT
import com.digitalasset.canton.participant.LocalOffset
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.{
  EventLogId,
  MultiDomainEventLogTest,
  TransferStore,
}
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.participant.sync.TimestampedEvent.EventId
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class MultiDomainEventLogTestInMemory extends MultiDomainEventLogTest {

  private val eventsRef: AtomicReference[Map[(EventLogId, LocalOffset), TimestampedEvent]] =
    new AtomicReference(Map.empty)

  override def storeEventsToSingleDimensionEventLogs(
      events: Seq[(EventLogId, TimestampedEvent)]
  ): Future[Unit] = {
    val rawEventsMap: Map[(EventLogId, LocalOffset), Seq[(EventLogId, TimestampedEvent)]] =
      events.groupBy { case (id, tsEvent) =>
        (id, tsEvent.localOffset)
      }
    val eventsMap: Map[(EventLogId, LocalOffset), TimestampedEvent] =
      rawEventsMap.map { case (key, events) =>
        val (_, event) = events.loneElement
        key -> event
      }
    eventsRef.set(eventsMap)
    Future.unit
  }

  protected override def cleanUpEventLogs(): Unit = ()

  private def lookupEvent(id: EventLogId, localOffset: LocalOffset): Future[TimestampedEvent] =
    Future.successful(eventsRef.get()(id -> localOffset))

  private val offsetLooker = new InMemoryOffsetsLookup {
    override def lookupOffsetsBetween(
        id: EventLogId
    )(fromExclusive: Option[LocalOffset], upToInclusive: Option[LocalOffset])(implicit
        executionContext: ExecutionContext,
        traceContext: TraceContext,
    ): Future[Seq[LocalOffset]] = {
      def offsetFilter(offset: LocalOffset): Boolean =
        fromExclusive.forall(_ < offset) && upToInclusive.forall(offset <= _)

      Future.successful {
        eventsRef
          .get()
          .collect {
            case ((`id`, offset), _) if offsetFilter(offset) => offset
          }
          .toSeq
          .sorted
      }
    }
  }

  private def domainIdOfEventId(eventId: EventId): OptionT[Future, (EventLogId, LocalOffset)] = {
    val resultO = eventsRef.get().collectFirst {
      case (eventLogIdAndLocalOffset, event) if event.eventId.contains(eventId) =>
        eventLogIdAndLocalOffset
    }
    OptionT(Future.successful(resultO))
  }

  override protected def transferStores: Map[TargetDomainId, TransferStore] = domainIds.map {
    domainId =>
      val targetDomainId = TargetDomainId(domainId)
      val transferStore = new InMemoryTransferStore(
        targetDomainId,
        loggerFactory,
      )

      targetDomainId -> transferStore
  }.toMap

  "MultiDomainEventLogTestInMemory" should {
    behave like multiDomainEventLog {
      new InMemoryMultiDomainEventLog(
        _ => lookupEvent,
        offsetLooker,
        _ => domainIdOfEventId,
        _,
        ParticipantTestMetrics,
        domainId =>
          transferStores.get(domainId).toRight(s"Cannot find transfer store for domain $domainId"),
        indexedStringStore,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )
    }
  }
}
