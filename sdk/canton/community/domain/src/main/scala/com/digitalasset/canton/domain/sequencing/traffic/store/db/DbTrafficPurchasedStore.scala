// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store.db

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficPurchasedStore
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.traffic.TrafficPurchased
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.BatchAggregator

import scala.concurrent.{ExecutionContext, Future}

class DbTrafficPurchasedStore(
    batchAggregatorConfig: BatchAggregatorConfig,
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends TrafficPurchasedStore
    with DbStore {

  import Member.DbStorageImplicits.*
  import storage.api.*

  // Batch aggregator to improve efficiency of storing traffic purchased entry updates
  private val batchAggregator = {
    val processor = new BatchAggregator.Processor[TrafficPurchased, Unit] {
      override val kind: String = "traffic purchased entry updates"
      override val logger: TracedLogger = DbTrafficPurchasedStore.this.logger
      override def executeBatch(items: NonEmpty[Seq[Traced[TrafficPurchased]]])(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): Future[Iterable[Unit]] = {
        val insertSql = storage.profile match {
          case Profile.H2(_) =>
            // H2 does not support on conflict with parameters, and Postgres needs them. So use the merge syntax for H2 instead
            "MERGE INTO seq_traffic_control_balance_updates AS target USING (VALUES (?, ?, ?, ?)) AS source (member, sequencing_timestamp, balance, serial)" +
              " ON target.member = source.member AND target.sequencing_timestamp = source.sequencing_timestamp" +
              " WHEN MATCHED AND source.serial > target.serial THEN UPDATE SET target.balance = source.balance, target.serial = source.serial" +
              " WHEN NOT MATCHED THEN INSERT (member, sequencing_timestamp, balance, serial) VALUES (source.member, source.sequencing_timestamp, source.balance, source.serial)"
          case _ =>
            "insert into seq_traffic_control_balance_updates (member, sequencing_timestamp, balance, serial) values (?, ?, ?, ?)" +
              " on conflict(member, sequencing_timestamp) do update set balance = excluded.balance, serial = excluded.serial where excluded.serial > seq_traffic_control_balance_updates.serial"
        }

        storage
          .queryAndUpdate(
            DbStorage.bulkOperation_(insertSql, items, storage.profile) { pp => balance =>
              pp >> balance.value.member
              pp >> balance.value.sequencingTimestamp
              pp >> balance.value.extraTrafficPurchased
              pp >> balance.value.serial
            },
            functionFullName,
          )(traceContext, callerCloseContext)
          .map(_ => Seq.fill(items.size)(()))
      }
      override def prettyItem: Pretty[TrafficPurchased] = implicitly
    }

    BatchAggregator(processor, batchAggregatorConfig)
  }

  override def store(
      trafficPurchased: TrafficPurchased
  )(implicit traceContext: TraceContext): Future[Unit] = {
    batchAggregator.run(trafficPurchased)
  }

  override def lookup(
      member: Member
  )(implicit traceContext: TraceContext): Future[Seq[TrafficPurchased]] = {
    val query =
      sql"select member, sequencing_timestamp, balance, serial from seq_traffic_control_balance_updates where member = $member order by sequencing_timestamp asc"
    storage.query(query.as[TrafficPurchased], functionFullName)
  }

  override def lookupLatestBeforeInclusive(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TrafficPurchased]] = {
    val query =
      sql"""select member, sequencing_timestamp, balance, serial
            from
              (select member, sequencing_timestamp, balance, serial,
                      rank() over (partition by member order by sequencing_timestamp desc) as pos
               from seq_traffic_control_balance_updates
               where sequencing_timestamp <= $timestamp
              ) as with_pos
            where pos = 1
           """

    storage.query(query.as[TrafficPurchased], functionFullName)
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
              from seq_traffic_control_balance_updates
              where sequencing_timestamp <= $upToExclusive
              group by member
            )
            delete from seq_traffic_control_balance_updates
            where (member, sequencing_timestamp) in (
              select purchased.member, purchased.sequencing_timestamp
              from last_before_pruning_timestamp last
              join seq_traffic_control_balance_updates purchased
              on purchased.member = last.member
              where purchased.sequencing_timestamp < last.sequencing_timestamp
            )
            """

    storage.update(deleteQuery, functionFullName).map { pruned =>
      s"Removed $pruned traffic purchased entries"
    }
  }

  override def maxTsO(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]] = {
    val query =
      sql"select max(sequencing_timestamp) from seq_traffic_control_balance_updates"

    storage.query(query.as[Option[CantonTimestamp]].headOption, functionFullName).map(_.flatten)
  }

  override def setInitialTimestamp(
      cantonTimestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val query =
      sqlu"insert into seq_traffic_control_initial_timestamp (initial_timestamp) values ($cantonTimestamp) on conflict do nothing"

    storage.update_(query, functionFullName)
  }

  override def getInitialTimestamp(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = {
    // TODO(i17640): figure out if / how we really want to handle multiple initial timestamps
    val query =
      sql"select initial_timestamp from seq_traffic_control_initial_timestamp order by initial_timestamp desc limit 1"

    storage.query(query.as[CantonTimestamp].headOption, functionFullName)
  }
}
