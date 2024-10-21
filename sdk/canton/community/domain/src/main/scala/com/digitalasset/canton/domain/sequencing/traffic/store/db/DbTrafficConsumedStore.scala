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
      trafficUpdates: Seq[TrafficConsumed]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val insertSql =
      """insert into seq_traffic_control_consumed_journal (member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost)
             values (?, ?, ?, ?, ?)
             on conflict do nothing"""

    val bulkInsert = DbStorage
      .bulkOperation(insertSql, trafficUpdates, storage.profile) { pp => trafficConsumed =>
        val TrafficConsumed(
          member,
          sequencingTimestamp,
          extraTrafficConsumed,
          baseTrafficRemainder,
          lastConsumedCost,
        ) = trafficConsumed
        pp >> member
        pp >> sequencingTimestamp
        pp >> extraTrafficConsumed
        pp >> baseTrafficRemainder
        pp >> lastConsumedCost
      }
      .map(_.sum)
    storage
      .queryAndUpdateUnlessShutdown(bulkInsert, functionFullName)
      .map(updateCount => logger.debug(s"Stored $updateCount traffic consumed entries"))
      .onShutdown {
        logger.debug(
          "DbTrafficConsumedStore is shutting down, cancelling storing traffic consumed entries"
        )
      }
  }

  override def lookup(
      member: Member
  )(implicit traceContext: TraceContext): Future[Seq[TrafficConsumed]] = {
    val query =
      sql"""select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
            from seq_traffic_control_consumed_journal
            where member = $member
            order by sequencing_timestamp asc"""
    storage.query(query.as[TrafficConsumed], functionFullName)
  }

  override def lookupLast(
      member: Member
  )(implicit traceContext: TraceContext): Future[Option[TrafficConsumed]] = {
    val query =
      sql"""select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
           from seq_traffic_control_consumed_journal
           where member = $member
           order by sequencing_timestamp desc
           limit 1"""
    storage.querySingle(query.as[TrafficConsumed].headOption, functionFullName).value
  }

  override def lookupLatestBeforeInclusive(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TrafficConsumed]] = {
    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sql"""select m.member, tc.sequencing_timestamp, tc.extra_traffic_consumed, tc.base_traffic_remainder, tc.last_consumed_cost
            from sequencer_members m
            inner join lateral (
              select sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
              from seq_traffic_control_consumed_journal
              where member = m.member and sequencing_timestamp <= $timestamp
              order by member, sequencing_timestamp desc
              limit 1) tc
            on true"""
      case _ =>
        // H2 does't support lateral joins
        sql"""select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
                    from
                      (select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost,
                              rank() over (partition by member order by sequencing_timestamp desc) as pos
                       from seq_traffic_control_consumed_journal
                       where sequencing_timestamp <= $timestamp
                      ) as with_pos
                    where pos = 1
                   """
    }
    storage.query(query.as[TrafficConsumed], functionFullName)
  }

  def lookupLatestBeforeInclusiveForMember(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[TrafficConsumed]] = {
    val query =
      sql"""select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
            from seq_traffic_control_consumed_journal
            where sequencing_timestamp <= $timestamp and member = $member
            order by member, sequencing_timestamp desc
            limit 1""".as[TrafficConsumed].headOption

    storage.querySingle(query, functionFullName).value
  }

  override def lookupAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[TrafficConsumed]] = {
    val query =
      sql"""select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
           from seq_traffic_control_consumed_journal
           where member = $member and sequencing_timestamp = $timestamp
           limit 1"""
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
    val lookupQuery = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sql"""select m.member, tc.sequencing_timestamp
              from sequencer_members m
              inner join lateral (
                select sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
                from seq_traffic_control_consumed_journal
                where member = m.member and sequencing_timestamp <= $upToExclusive
                order by member, sequencing_timestamp desc
                limit 1) tc
              on true"""
      case _ =>
        sql"""select member, max(sequencing_timestamp) as sequencing_timestamp
              from seq_traffic_control_consumed_journal
              where sequencing_timestamp <= $upToExclusive
              group by member"""
    }

    val deleteQuery =
      """delete from seq_traffic_control_consumed_journal
        |where member = ? and sequencing_timestamp < ?
        |""".stripMargin
    val pruningQuery = for {
      membersTimestamps <- lookupQuery.as[(Member, CantonTimestamp)]
      deletedTotalCount <- DbStorage
        .bulkOperation(deleteQuery, membersTimestamps, storage.profile) { pp => memberTimestamp =>
          val (member, timestamp) = memberTimestamp
          pp >> member
          pp >> timestamp
        }
        .map(_.sum)
    } yield deletedTotalCount

    storage
      .queryAndUpdateUnlessShutdown(pruningQuery, functionFullName)
      .onShutdown {
        logger.debug(
          "DbTrafficConsumedStore is shutting down, cancelling pruning traffic consumed entries"
        )
        0
      }
      .map { pruned =>
        s"Removed $pruned traffic consumed entries"
      }
  }
}
