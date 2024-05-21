// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.LocalOffset
import com.digitalasset.canton.participant.store.db.DbSingleDimensionEventLog
import com.digitalasset.canton.participant.store.memory.InMemorySingleDimensionEventLog
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.participant.sync.TimestampedEvent.{EventId, TransactionEventId}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.digitalasset.canton.{LedgerTransactionId, checked}
import slick.jdbc.SetParameter

import scala.collection.SortedMap
import scala.concurrent.{ExecutionContext, Future}

/** Read-only interface of [[SingleDimensionEventLog]] */
trait SingleDimensionEventLogLookup {
  def eventAt(offset: LocalOffset)(implicit
      traceContext: TraceContext
  ): OptionT[Future, TimestampedEvent]

  def lookupEventRange(
      fromExclusive: Option[LocalOffset],
      toInclusive: Option[LocalOffset],
      fromTimestampInclusive: Option[CantonTimestamp],
      toTimestampInclusive: Option[CantonTimestamp],
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LocalOffset, TimestampedEvent]]
}

/** An event log for a single domain or for a domain-independent events (such as package uploads).
  * Supports out-of-order publication of events.
  */
trait SingleDimensionEventLog[+Id <: EventLogId] extends SingleDimensionEventLogLookup {
  this: NamedLogging =>

  protected implicit def executionContext: ExecutionContext

  def id: Id

  /** Publishes an event.
    * @return [[scala.Left$]] if an event with the same event ID has been published with a different (unpruned)
    *         [[com.digitalasset.canton.participant.LocalOffset]]. Returns the conflicting event.
    * @throws java.lang.IllegalArgumentException if a different event has already been published with the same
    *                                            [[com.digitalasset.canton.participant.LocalOffset]]
    */
  private[store] def insertUnlessEventIdClash(event: TimestampedEvent)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TimestampedEvent, Unit] = EitherT {
    insertsUnlessEventIdClash(Seq(event)).map {
      _.headOption.getOrElse(
        ErrorUtil.internalError(
          new RuntimeException(
            "insertsUnlessEventIdClash returned an empty sequence for a singleton list"
          )
        )
      )
    }
  }

  /** Publishes many events.
    * @return For each event in `events` in the same order:
    *         [[scala.Left$]] of the conficting event
    *         if an event with the same event ID has been published with a different (unpruned)
    *         [[com.digitalasset.canton.participant.LocalOffset]].
    *         [[scala.Right$]] if the event has been successfully inserted or has already been present in the event log
    * @throws java.lang.IllegalArgumentException if a different event has already been published with the same
    *                                            [[com.digitalasset.canton.participant.LocalOffset]]
    *                                            as one of the events in `events`.
    */
  def insertsUnlessEventIdClash(events: Seq[TimestampedEvent])(implicit
      traceContext: TraceContext
  ): Future[Seq[Either[TimestampedEvent, Unit]]]

  /** Publishes an event and fails upon an event ID clash.
    * @throws java.lang.IllegalArgumentException if a different event has already been published with the same
    *                                            [[com.digitalasset.canton.participant.LocalOffset]] or
    *                                            an event with the same [[com.digitalasset.canton.participant.sync.TimestampedEvent.EventId]]
    *                                            has been published with a different (unpruned) [[com.digitalasset.canton.participant.LocalOffset]].
    */
  def insert(event: TimestampedEvent)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    insertUnlessEventIdClash(event).valueOr(eventWithSameId =>
      ErrorUtil.internalError(
        new IllegalArgumentException(
          show"Unable to insert event, as the eventId id ${event.eventId.showValue} has " +
            show"already been inserted with offset ${eventWithSameId.localOffset}."
        )
      )
    )

  /** Publishes events and fails upon an event ID clash.
    * @throws java.lang.IllegalArgumentException if a one of the event has already been published with the same
    *                                            [[com.digitalasset.canton.participant.LocalOffset]] or
    *                                            an event with the same [[com.digitalasset.canton.participant.sync.TimestampedEvent.EventId]]
    *                                            has been published with a different (unpruned) [[com.digitalasset.canton.participant.LocalOffset]].
    */
  def insert(events: Seq[TimestampedEvent])(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    insertsUnlessEventIdClash(events).map { results =>
      val clashes = results.collect { case Left(eventWithSameId) =>
        (eventWithSameId.eventId, eventWithSameId.localOffset)
      }

      if (clashes.nonEmpty) {
        ErrorUtil.internalError(
          new IllegalArgumentException(
            show"Unable to insert events, as some of the events were already inserted: $clashes"
          )
        )
      }
    }
  }

  def prune(beforeAndIncluding: LocalOffset)(implicit traceContext: TraceContext): Future[Unit]

  private[store] def lastOffset(implicit traceContext: TraceContext): OptionT[Future, LocalOffset]

  def eventById(eventId: EventId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, TimestampedEvent]

  def eventByTransactionId(transactionId: LedgerTransactionId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, TimestampedEvent] =
    eventById(TransactionEventId(transactionId))

  /** Returns whether there exists an event in the event log with an [[com.digitalasset.canton.participant.LocalOffset]]
    * of at most `localOffset` whose timestamp is at least `timestamp`.
    *
    * In an event logs where timestamps need not increase with offsets,
    * this can be used to check that whether there are events with lower offsets and larger timestamps.
    */
  private[store] def existsBetween(
      timestampInclusive: CantonTimestamp,
      localOffsetInclusive: LocalOffset,
  )(implicit
      traceContext: TraceContext
  ): Future[Boolean]

  /** Deletes all events whose local offset is greater than `exclusive`.
    * This operation need not execute atomically.
    */
  def deleteAfter(exclusive: LocalOffset)(implicit traceContext: TraceContext): Future[Unit]
}

sealed trait EventLogId extends PrettyPrinting with Product with Serializable {
  def index: Int
  def context: MetricsContext
}

object EventLogId {

  implicit val setParameterNamespace: SetParameter[EventLogId] = (v, pp) => pp.setInt(v.index)

  def fromDbLogIdOT(context: String, indexedStringStore: IndexedStringStore)(index: Int)(implicit
      ec: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): OptionT[Future, EventLogId] =
    if (index <= 0) {
      OptionT.some(checked {
        // This is safe to call, because index <= 0
        ParticipantEventLogId.tryCreate(index)
      })
    } else
      IndexedDomain.fromDbIndexOT(context, indexedStringStore)(index).map(DomainEventLogId)

  final case class DomainEventLogId(id: IndexedDomain) extends EventLogId {
    override def pretty: Pretty[DomainEventLogId] = prettyOfParam(_.id.item)

    override def index: Int = id.index

    def domainId: DomainId = id.item

    def context: MetricsContext = MetricsContext(
      "logId" -> (if (index == 0) domainId.show else domainId.show + "_" + index)
    )

  }

  def forDomain(indexedStringStore: IndexedStringStore)(id: DomainId)(implicit
      ec: ExecutionContext
  ): Future[DomainEventLogId] =
    IndexedDomain.indexed(indexedStringStore)(id).map(DomainEventLogId)

  sealed abstract case class ParticipantEventLogId(override val index: Int) extends EventLogId {
    require(
      index <= 0,
      s"Illegal index $index. The index must not be positive to prevent clashes with domain event log ids.",
    )

    override def pretty: Pretty[ParticipantEventLogId] = prettyOfClass(param("index", _.index))

    def context: MetricsContext = MetricsContext(
      "logId" -> (if (index == 0) "participant" else "participant_" + index)
    )

  }

  object ParticipantEventLogId {

    /** @throws java.lang.IllegalArgumentException if `index > 0`.
      */
    def tryCreate(index: Int): ParticipantEventLogId = new ParticipantEventLogId(index) {}
  }
}

object SingleDimensionEventLog {

  def apply[Id <: EventLogId](
      id: Id,
      storage: Storage,
      indexedStringStore: IndexedStringStore,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): SingleDimensionEventLog[Id] =
    storage match {
      case _: MemoryStorage => new InMemorySingleDimensionEventLog[Id](id, loggerFactory)
      case dbStorage: DbStorage =>
        new DbSingleDimensionEventLog[Id](
          id,
          dbStorage,
          indexedStringStore,
          releaseProtocolVersion,
          timeouts,
          loggerFactory,
        )
    }

}
