// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.traffic.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.traffic.TrafficConsumed
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.TrafficConsumedStore
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.ExecutionContext

/** Stores traffic consumed entries in a database. The store is organized as a journal, every
  * traffic consumption is added as a new row.
  */
class DbTrafficConsumedStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    batchingConfig: BatchingConfig,
)(implicit executionContext: ExecutionContext)
    extends TrafficConsumedStore
    with DbStore {

  import Member.DbStorageImplicits.*
  import storage.api.*

  override def store(
      trafficUpdates: Seq[TrafficConsumed]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
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
      .queryAndUpdate(bulkInsert, functionFullName)
      .map(updateCount => logger.debug(s"Stored $updateCount traffic consumed entries"))
      .tapOnShutdown {
        logger.debug(
          "DbTrafficConsumedStore is shutting down, cancelling storing traffic consumed entries"
        )
      }
  }

  override def lookup(
      member: Member
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[TrafficConsumed]] = {
    val query =
      sql"""select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
            from seq_traffic_control_consumed_journal
            where member = $member
            order by sequencing_timestamp asc"""
    storage.query(query.as[TrafficConsumed], functionFullName)
  }

  override def lookupLast(
      member: Member
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[TrafficConsumed]] = {
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
  ): FutureUnlessShutdown[Seq[TrafficConsumed]] = {
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
  ): FutureUnlessShutdown[Option[TrafficConsumed]] = {
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
  ): FutureUnlessShutdown[Option[TrafficConsumed]] = {
    val query =
      sql"""select member, sequencing_timestamp, extra_traffic_consumed, base_traffic_remainder, last_consumed_cost
           from seq_traffic_control_consumed_journal
           where member = $member and sequencing_timestamp = $timestamp
           limit 1"""
    storage.querySingle(query.as[TrafficConsumed].headOption, functionFullName).value
  }

  override def pruneBelowExclusive(
      upToExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[String] = {
    // We need to delete all rows with sequencing_timestamp below the closest row to upToExclusive, by member.
    // That is because the closest row contains the value which are valid at upToExclusive. So even if it's below
    // upToExclusive, we need to keep it.
    // To do that we first find the latest timestamp for all members before the pruning timestamp.
    // Then we delete all rows below that timestamp for each member,
    // using parallelized batched non-transactional DbStorage.bulkOperation.
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

    val deletedTotalCountFUS = for {
      membersTimestamps <- storage.query(
        lookupQuery.as[(Member, CantonTimestamp)],
        functionFullName,
      )
      deletedTotalCount <-
        if (membersTimestamps.isEmpty) {
          FutureUnlessShutdown.pure(0)
        } else {
          MonadUtil
            .batchedSequentialTraverse(batchingConfig.parallelism, batchingConfig.maxItemsInBatch)(
              membersTimestamps
            ) { membersTimestampsChunk =>
              val bulkDelete = DbStorage
                .bulkOperation(
                  deleteQuery,
                  membersTimestampsChunk,
                  storage.profile,
                  transactional = false,
                ) { pp => memberTimestamp =>
                  val (member, timestamp) = memberTimestamp
                  pp >> member
                  pp >> timestamp
                }
                .map(_.toSeq)
              storage.queryAndUpdate(bulkDelete, functionFullName)
            }
            .map(_.sum)
        }
    } yield deletedTotalCount

    deletedTotalCountFUS
      .tapOnShutdown {
        logger.debug(
          "DbTrafficConsumedStore is shutting down, cancelling pruning traffic consumed entries"
        )
      }
      .transformOnShutdown(0)
      .map { pruned =>
        s"Removed $pruned traffic consumed entries"
      }
  }

  override def deleteRecordsPastTimestamp(
      timestampExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val deleteQuery =
      s"""delete from seq_traffic_control_consumed_journal
        |where member = ? and sequencing_timestamp > ?
        |""".stripMargin

    val cleanUpQuery = for {
      // First lookup all members
      members <- sql"select member from sequencer_members".as[Member]
      // Then delete all consumed record above the timestamp for each member
      // This force the DB to use the index on (member, sequencing_timestamp)
      deletedCount <- DbStorage
        .bulkOperation(deleteQuery, members, storage.profile) { pp => member =>
          pp >> member
          pp >> timestampExclusive
        }
        .map(_.sum)
    } yield deletedCount

    storage
      .queryAndUpdate(cleanUpQuery.transactionally, functionFullName)
      .transformOnShutdown(0)
      .map { deletedCount =>
        logger.debug(
          s"Deleted $deletedCount traffic consumed with timestamps > $timestampExclusive"
        )
      }
  }
}
