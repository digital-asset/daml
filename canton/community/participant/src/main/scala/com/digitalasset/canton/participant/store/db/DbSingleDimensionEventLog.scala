// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.OptionT
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.LocalOffset
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.participant.sync.TimestampedEvent.EventId
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.jdbc.*

import scala.collection.{SortedMap, mutable}
import scala.concurrent.{ExecutionContext, Future}

class DbSingleDimensionEventLog[+Id <: EventLogId](
    override val id: Id,
    override protected val storage: DbStorage,
    indexedStringStore: IndexedStringStore,
    releaseProtocolVersion: ReleaseProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit override protected val executionContext: ExecutionContext)
    extends SingleDimensionEventLog[Id]
    with DbStore {

  import ParticipantStorageImplicits.*
  import storage.api.*
  import storage.converters.*

  protected val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("single-dimension-event-log")

  private def log_id: Int = id.index

  private implicit val setParameterTraceContext: SetParameter[SerializableTraceContext] =
    SerializableTraceContext.getVersionedSetParameter(releaseProtocolVersion.v)
  private implicit val setParameterSerializableLedgerSyncEvent
      : SetParameter[SerializableLedgerSyncEvent] =
    SerializableLedgerSyncEvent.getVersionedSetParameter

  override def insertsUnlessEventIdClash(
      events: Seq[TimestampedEvent]
  )(implicit traceContext: TraceContext): Future[Seq[Either[TimestampedEvent, Unit]]] = {
    idempotentInserts(events).flatMap { insertResult =>
      insertResult.parTraverse {
        case right @ Right(()) => Future.successful(right)
        case Left(event) =>
          event.eventId match {
            case None => cannotInsertEvent(event)
            case Some(eventId) =>
              val e = eventById(eventId)
              e.toLeft(cannotInsertEvent(event)).value
          }
      }
    }
  }

  private def idempotentInserts(
      events: Seq[TimestampedEvent]
  )(implicit traceContext: TraceContext): Future[List[Either[TimestampedEvent, Unit]]] =
    processingTime.event {
      for {
        rowCounts <- rawInserts(events)
        _ = ErrorUtil.requireState(
          rowCounts.length == events.size,
          "Event insertion did not produce a result for each event",
        )
        insertionResults = rowCounts.toList.zip(events).map { case (rowCount, event) =>
          Either.cond(rowCount == 1, (), event)
        }
        checkResults <- insertionResults.parTraverse {
          case Right(()) => Future.successful(Right(()))
          case Left(event) =>
            logger.info(
              show"Insertion into event log at offset ${event.localOffset} skipped. Checking the reason..."
            )
            eventAt(event.localOffset).fold(Either.left[TimestampedEvent, Unit](event)) {
              existingEvent =>
                if (existingEvent.normalized == event.normalized) {
                  logger.info(
                    show"The event at offset ${event.localOffset} has already been inserted. Nothing to do."
                  )
                  Right(())
                } else
                  ErrorUtil.internalError(
                    new IllegalArgumentException(
                      show"""Unable to overwrite an existing event. Aborting.
                            |Existing event: ${existingEvent}

                            |New event: $event""".stripMargin
                    )
                  )
            }
        }
      } yield checkResults
    }

  private def rawInserts(
      events: Seq[TimestampedEvent]
  )(implicit traceContext: TraceContext): Future[Array[Int]] = {
    // resolve associated domain-id
    val eventsWithAssociatedDomainIdF = events.parTraverse { event =>
      event.eventId.flatMap(_.associatedDomain) match {
        case Some(domainId) =>
          IndexedDomain.indexed(indexedStringStore)(domainId).map(indexed => (event, Some(indexed)))
        case None => Future.successful((event, None))
      }
    }

    eventsWithAssociatedDomainIdF.flatMap { eventsWithAssociatedDomainId =>
      processingTime.event {
        val dbio = storage.profile match {
          case _: DbStorage.Profile.Oracle =>
            val query =
              """merge into event_log e
                 using dual on ( (e.event_id = ?) or (e.log_id = ? and e.local_offset_effective_time = ? and e.local_offset_discriminator = ? and e.local_offset_tie_breaker = ?))
                 when not matched then
                 insert (log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, ts, request_sequencer_counter, event_id, associated_domain, content, trace_context)
                 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            DbStorage.bulkOperation(
              query,
              eventsWithAssociatedDomainId,
              storage.profile,
            ) { pp => eventWithDomain =>
              val (event, associatedDomainIdO) = eventWithDomain

              pp >> event.eventId
              pp >> log_id
              pp >> event.localOffset
              pp >> log_id
              pp >> event.localOffset
              pp >> event.timestamp
              pp >> event.requestSequencerCounter
              pp >> event.eventId
              pp >> associatedDomainIdO.map(_.index)
              pp >> SerializableLedgerSyncEvent(event.event, releaseProtocolVersion.v)
              pp >> SerializableTraceContext(event.traceContext)
            }
          case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
            val query =
              """insert into event_log (log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, ts, request_sequencer_counter, event_id, associated_domain, content, trace_context)
               values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               on conflict do nothing"""
            DbStorage.bulkOperation(query, eventsWithAssociatedDomainId, storage.profile) {
              pp => eventWithDomain =>
                val (event, associatedDomainIdO) = eventWithDomain

                pp >> log_id
                pp >> event.localOffset
                pp >> event.timestamp
                pp >> event.requestSequencerCounter
                pp >> event.eventId
                pp >> associatedDomainIdO.map(_.index)
                pp >> SerializableLedgerSyncEvent(event.event, releaseProtocolVersion.v)
                pp >> SerializableTraceContext(event.traceContext)
            }
        }
        storage.queryAndUpdate(dbio, functionFullName)
      }
    }
  }

  private def cannotInsertEvent(event: TimestampedEvent): Nothing = {
    implicit val traceContext: TraceContext = event.traceContext
    val withId = event.eventId.fold("")(id => s" with id $id")
    ErrorUtil.internalError(
      new IllegalStateException(
        show"Unable to insert event at offset ${event.localOffset}${withId.unquoted}.\n$event"
      )
    )
  }

  override def prune(
      beforeAndIncluding: LocalOffset
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.event {
      val query = storage.profile match {
        case _: Profile.H2 | _: Profile.Postgres =>
          sqlu"""
            delete from event_log
            where
              log_id = $log_id
              and (local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker) <= (${beforeAndIncluding.effectiveTime}, ${beforeAndIncluding.discriminator}, ${beforeAndIncluding.tieBreaker})"""

        case _: Profile.Oracle =>
          val t = beforeAndIncluding.effectiveTime
          val disc = beforeAndIncluding.discriminator
          val tie = beforeAndIncluding.tieBreaker

          sqlu"""
            delete from event_log
            where
              log_id = $log_id
              and ((local_offset_effective_time<$t) or (local_offset_effective_time=$t and local_offset_discriminator<$disc) or (local_offset_effective_time=$t and local_offset_discriminator=$disc and local_offset_tie_breaker<=$tie))"""
      }

      storage.update_(query, functionFullName)
    }

  override def lookupEventRange(
      fromExclusive: Option[LocalOffset],
      toInclusive: Option[LocalOffset],
      fromTimestampInclusive: Option[CantonTimestamp],
      toTimestampInclusive: Option[CantonTimestamp],
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LocalOffset, TimestampedEvent]] = {

    processingTime.event {
      DbSingleDimensionEventLog.lookupEventRange(
        storage = storage,
        eventLogId = id,
        fromExclusive = fromExclusive,
        toInclusive = toInclusive,
        fromTimestampInclusive = fromTimestampInclusive,
        toTimestampInclusive = toTimestampInclusive,
        limit = limit,
      )
    }
  }

  override def eventAt(
      offset: LocalOffset
  )(implicit traceContext: TraceContext): OptionT[Future, TimestampedEvent] =
    processingTime.optionTEvent {
      val query = storage.profile match {
        case _: Profile.H2 | _: Profile.Postgres =>
          sql"""select local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, request_sequencer_counter, event_id, content, trace_context
              from event_log
              where log_id = $log_id and (local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker)=(${offset.effectiveTime}, ${offset.discriminator}, ${offset.tieBreaker})"""
        case _: Profile.Oracle =>
          sql"""select /*+ INDEX (event_log pk_event_log) */
              local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, request_sequencer_counter, event_id, content, trace_context
              from event_log
              where log_id = $log_id and local_offset_effective_time=${offset.effectiveTime} and local_offset_discriminator=${offset.discriminator} and local_offset_tie_breaker=${offset.tieBreaker}"""
      }

      storage
        .querySingle(
          query.as[TimestampedEvent].headOption,
          functionFullName,
        )
    }

  override def lastOffset(implicit traceContext: TraceContext): OptionT[Future, LocalOffset] =
    processingTime.optionTEvent {
      storage.querySingle(
        sql"""select local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker from event_log where log_id = $log_id
              order by local_offset_effective_time desc, local_offset_discriminator desc, local_offset_tie_breaker desc #${storage
            .limit(1)}"""
          .as[LocalOffset]
          .headOption,
        functionFullName,
      )
    }

  override def eventById(
      eventId: EventId
  )(implicit traceContext: TraceContext): OptionT[Future, TimestampedEvent] =
    processingTime.optionTEvent {
      storage
        .querySingle(
          sql"""select local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, request_sequencer_counter, event_id, content, trace_context
              from event_log
              where log_id = $log_id and event_id = $eventId"""
            .as[TimestampedEvent]
            .headOption,
          functionFullName,
        )
    }

  override def existsBetween(
      timestampInclusive: CantonTimestamp,
      localOffsetInclusive: LocalOffset,
  )(implicit traceContext: TraceContext): Future[Boolean] = {
    import DbStorage.Implicits.BuilderChain.*

    processingTime.event {
      val queryLocalOffset =
        DbSingleDimensionEventLog.localOffsetComparison(storage)("<=", localOffsetInclusive)

      val query = sql"""
        select 1 from event_log where
          log_id = $log_id
          """ ++ queryLocalOffset ++ sql" and ts >= $timestampInclusive #${storage.limit(1)}"

      storage.query(query.as[Int].headOption, "exists between").map(_.isDefined)
    }
  }

  override def deleteAfter(
      exclusive: LocalOffset
  )(implicit traceContext: TraceContext): Future[Unit] = {

    val query = storage.profile match {
      case _: Profile.H2 | _: Profile.Postgres =>
        sqlu"""
          delete from event_log
          where
            log_id = $log_id
            and (local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker) > (${exclusive.effectiveTime}, ${exclusive.discriminator}, ${exclusive.tieBreaker})"""

      case _: Profile.Oracle =>
        val t = exclusive.effectiveTime
        val disc = exclusive.discriminator
        val tie = exclusive.tieBreaker

        sqlu"""
          delete from event_log
          where
            log_id = $log_id
            and ((local_offset_effective_time>$t) or (local_offset_effective_time=$t and local_offset_discriminator>$disc) or (local_offset_effective_time=$t and local_offset_discriminator=$disc and local_offset_tie_breaker>$tie))"""
    }

    processingTime.event {
      storage.update_(query, functionFullName)
    }
  }
}

object DbSingleDimensionEventLog {

  /** @param op One comparison operator (<, <=, >, >=)
    * @param offset Local offset to compare to
    * @return
    */
  private def localOffsetComparison(
      storage: DbStorage
  )(op: String, offset: LocalOffset): canton.SQLActionBuilder = {
    import storage.api.*

    val t = offset.effectiveTime
    val disc = offset.discriminator
    val tie = offset.tieBreaker

    storage.profile match {
      case _: Profile.H2 | _: Profile.Postgres =>
        sql" and ((local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker) #$op ($t, $disc, $tie))"

      case _: Profile.Oracle =>
        sql" and ((local_offset_effective_time#$op$t) or (local_offset_effective_time=$t and local_offset_discriminator#$op$disc) or (local_offset_effective_time=$t and local_offset_discriminator=$disc and local_offset_tie_breaker#$op$tie))"
    }
  }

  private[store] def lookupEventRange(
      storage: DbStorage,
      eventLogId: EventLogId,
      fromExclusive: Option[LocalOffset],
      toInclusive: Option[LocalOffset],
      fromTimestampInclusive: Option[CantonTimestamp],
      toTimestampInclusive: Option[CantonTimestamp],
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
      closeContext: CloseContext,
  ): Future[SortedMap[LocalOffset, TimestampedEvent]] = {
    import DbStorage.Implicits.BuilderChain.*
    import ParticipantStorageImplicits.*
    import TimestampedEvent.getResultTimestampedEvent
    import storage.api.*
    import storage.converters.*

    val filters = List(
      fromExclusive.map(localOffsetComparison(storage)(">", _)),
      toInclusive.map(localOffsetComparison(storage)("<=", _)),
      fromTimestampInclusive.map(n => sql" and ts >= $n"),
      toTimestampInclusive.map(n => sql" and ts <= $n"),
    ).flattenOption.intercalate(sql"")

    for {
      eventsVector <- storage.query(
        (sql"""select local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, request_sequencer_counter, event_id, content, trace_context
                 from event_log
                 where log_id = ${eventLogId.index}""" ++ filters ++
          sql""" order by local_offset_effective_time asc, local_offset_discriminator asc, local_offset_tie_breaker asc #${storage
              .limit(limit.getOrElse(Int.MaxValue))}""")
          .as[TimestampedEvent]
          .map(_.map { event => event.localOffset -> event }),
        functionFullName,
      )
    } yield {
      val result = new mutable.TreeMap[LocalOffset, TimestampedEvent]()
      result ++= eventsVector
      result
    }
  }
}
