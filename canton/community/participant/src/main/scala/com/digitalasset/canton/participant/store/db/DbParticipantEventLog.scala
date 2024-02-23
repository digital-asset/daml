// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.RequestOffset
import com.digitalasset.canton.participant.store.EventLogId.ParticipantEventLogId
import com.digitalasset.canton.participant.store.ParticipantEventLog
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class DbParticipantEventLog(
    id: ParticipantEventLogId,
    override val storage: DbStorage,
    indexedStringStore: IndexedStringStore,
    releaseProtocolVersion: ReleaseProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends DbSingleDimensionEventLog[ParticipantEventLogId](
      id,
      storage,
      indexedStringStore,
      releaseProtocolVersion,
      timeouts,
      loggerFactory,
    )
    with ParticipantEventLog {

  import storage.api.*
  import storage.converters.*
  import ParticipantStorageImplicits.*

  override def firstEventWithAssociatedDomainAtOrAfter(
      associatedDomain: DomainId,
      atOrAfter: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Option[TimestampedEvent]] = {
    IndexedDomain.indexed(indexedStringStore)(associatedDomain).flatMap { associatedDomainIndex =>
      {
        // Use #$ instead of $ for ParticipantEventLogId.log_id so that it shows up as a literal string
        // in the prepared statement instead of a ?. This ensures that the query planner can pick the partial index.
        //
        // Note that the index will only be used for ParticipantEventLog.ProductionParticipantEventLogId!
        val query = storage.profile match {
          case _: DbStorage.Profile.Oracle =>
            sql"""
        select local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, request_sequencer_counter, event_id, content, trace_context from event_log
        where (case when log_id = '#${id.index}' then associated_domain end) = $associatedDomainIndex
          and (case when log_id = '#${id.index}' and associated_domain is not null then ts end) >= $atOrAfter
        order by local_offset_effective_time asc, local_offset_discriminator asc, local_offset_tie_breaker asc
        #${storage.limit(1)}
        """.as[TimestampedEvent]
          case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
            sql"""
        select local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, request_sequencer_counter, event_id, content, trace_context from event_log
        where log_id = '#${id.index}' and associated_domain is not null
          and associated_domain = $associatedDomainIndex and ts >= $atOrAfter
        order by (local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker) asc
        #${storage.limit(1)}
        """.as[TimestampedEvent]
        }
        storage.query(query.headOption, functionFullName)
      }
    }
  }

  override def nextLocalOffsets(
      count: NonNegativeInt
  )(implicit traceContext: TraceContext): Future[Seq[RequestOffset]] =
    if (count.unwrap > 0) {
      val query = storage.profile match {
        case _: DbStorage.Profile.Postgres =>
          sql"""select nextval('participant_event_publisher_local_offsets') from generate_series(1, #${count.unwrap})"""
            .as[RequestCounter]
        case _: DbStorage.Profile.Oracle =>
          sql"""select participant_event_publisher_local_offsets.nextval from (select level from dual connect by level <= #${count.unwrap})"""
            .as[RequestCounter]
        case _: DbStorage.Profile.H2 =>
          import DbStorage.Implicits.BuilderChain.*
          (sql"select nextval('participant_event_publisher_local_offsets') from (values " ++
            (1 to count.unwrap).toList.map(i => sql"(#$i)").intercalate(sql", ") ++
            sql")")
            .as[RequestCounter]
      }
      storage
        .queryAndUpdate(query, functionFullName)
        .map(_.map(RequestOffset(ParticipantEventLog.EffectiveTime, _)))
    } else Future.successful(Seq.empty)
}
