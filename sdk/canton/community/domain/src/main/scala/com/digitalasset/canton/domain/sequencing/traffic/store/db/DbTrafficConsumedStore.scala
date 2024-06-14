// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficConsumedStore
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.traffic.TrafficConsumed
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Stores traffic consumed entries in a database.
  * The store is organized as a journal, every traffic consumption is added as a new row.
  */
class DbTrafficConsumedStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends TrafficConsumedStore
    with DbStore {

  import Member.DbStorageImplicits.*
  import storage.api.*

  override def store(
      trafficConsumed: TrafficConsumed
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val insertSql =
      sqlu"""insert into seq_traffic_control_consumed_journal (member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder)
             values (${trafficConsumed.member}, ${trafficConsumed.sequencingTimestamp}, ${trafficConsumed.extraTrafficConsumed}, ${trafficConsumed.baseTrafficRemainder}) on conflict do nothing"""

    storage.update_(insertSql, functionFullName)
  }

  override def lookup(
      member: Member
  )(implicit traceContext: TraceContext): Future[Seq[TrafficConsumed]] = {
    val query =
      sql"select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder from seq_traffic_control_consumed_journal where member = $member order by sequencing_timestamp asc"
    storage.query(query.as[TrafficConsumed], functionFullName)
  }

  override def lookupLast(
      member: Member
  )(implicit traceContext: TraceContext): Future[Option[TrafficConsumed]] = {
    val query =
      sql"select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder from seq_traffic_control_consumed_journal where member = $member order by sequencing_timestamp desc"
    storage.querySingle(query.as[TrafficConsumed].headOption, functionFullName).value
  }

  override def lookupLatestBeforeInclusive(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TrafficConsumed]] = {
    val query =
      sql"""select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder
            from
              (select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder,
                      rank() over (partition by member order by sequencing_timestamp desc) as pos
               from seq_traffic_control_consumed_journal
               where sequencing_timestamp <= $timestamp
              ) as with_pos
            where pos = 1
           """

    storage.query(query.as[TrafficConsumed], functionFullName)
  }

  def lookupLatestBeforeInclusiveForMember(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[TrafficConsumed]] = {
    val query =
      sql"""select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder
            from
              (select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder,
                      rank() over (partition by member order by sequencing_timestamp desc) as pos
               from seq_traffic_control_consumed_journal
               where sequencing_timestamp <= $timestamp and member = $member
              ) as with_pos
            where pos = 1
           """.as[TrafficConsumed].headOption

    storage.querySingle(query, functionFullName).value
  }

  override def lookupAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[TrafficConsumed]] = {
    val query =
      sql"select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder from seq_traffic_control_consumed_journal where member = $member and sequencing_timestamp = $timestamp"
    storage.querySingle(query.as[TrafficConsumed].headOption, functionFullName).value
  }

  override def pruneBelowExclusive(
      upToExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[String] = {
    // We need to delete all rows with sequencing_timestamp below the closest row to upToExclusive, by member.
    // That is because the closest row contains the value which are valid at upToExclusive. So even if it's below
    // upToExclusive, we need to keep it.
    // To do that we first find the latest timestamp for all members before the pruning timestamp.
    // Then we delete all rows below that timestamp for each member.
    val deleteQuery =
      sqlu"""with last_before_pruning_timestamp(member, sequencing_timestamp) as (
              select member, max(sequencing_timestamp)
              from seq_traffic_control_consumed_journal
              where sequencing_timestamp <= $upToExclusive
              group by member
            )
            delete from seq_traffic_control_consumed_journal
            where (member, sequencing_timestamp) in (
              select consumed.member, consumed.sequencing_timestamp
              from last_before_pruning_timestamp last
              join seq_traffic_control_consumed_journal consumed
              on consumed.member = last.member
              where consumed.sequencing_timestamp < last.sequencing_timestamp
            )
            """
    storage.update(deleteQuery, functionFullName).map { pruned =>
      s"Removed $pruned traffic consumed entries"
    }
  }
}
