// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store.db

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.traffic.TrafficBalanceManager.TrafficBalance
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficBalanceStore
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.BatchAggregator

import scala.concurrent.{ExecutionContext, Future}

class DbTrafficBalanceStore(
    batchAggregatorConfig: BatchAggregatorConfig,
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends TrafficBalanceStore
    with DbStore {

  import Member.DbStorageImplicits.*
  import storage.api.*

  // Batch aggregator to improve efficiency of storing traffic balance updates
  private val batchAggregator = {
    val processor = new BatchAggregator.Processor[TrafficBalance, Unit] {
      override val kind: String = "traffic balance updates"
      override val logger: TracedLogger = DbTrafficBalanceStore.this.logger
      override def executeBatch(items: NonEmpty[Seq[Traced[TrafficBalance]]])(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): Future[Iterable[Unit]] = {
        val insertSql = storage.profile match {
          case Profile.H2(_) =>
            // H2 does not support on conflict with parameters, and Postgres needs them. So use the merge syntax for H2 instead
            "MERGE INTO sequencer_traffic_control_balance_updates AS target USING (VALUES (?, ?, ?, ?)) AS source (member, sequencing_timestamp, balance, serial)" +
              " ON target.member = source.member AND target.sequencing_timestamp = source.sequencing_timestamp" +
              " WHEN MATCHED AND source.serial > target.serial THEN UPDATE SET target.balance = source.balance, target.serial = source.serial" +
              " WHEN NOT MATCHED THEN INSERT (member, sequencing_timestamp, balance, serial) VALUES (source.member, source.sequencing_timestamp, source.balance, source.serial)"
          case _ =>
            "insert into sequencer_traffic_control_balance_updates (member, sequencing_timestamp, balance, serial) values (?, ?, ?, ?)" +
              " on conflict(member, sequencing_timestamp) do update set balance = excluded.balance, serial = excluded.serial where excluded.serial > sequencer_traffic_control_balance_updates.serial"
        }

        storage
          .queryAndUpdate(
            DbStorage.bulkOperation_(insertSql, items, storage.profile) { pp => balance =>
              pp >> balance.value.member
              pp >> balance.value.sequencingTimestamp
              pp >> balance.value.balance
              pp >> balance.value.serial
            },
            functionFullName,
          )(traceContext, callerCloseContext)
          .map(_ => Seq.fill(items.size)(()))
      }
      override def prettyItem: Pretty[TrafficBalance] = implicitly
    }

    BatchAggregator(processor, batchAggregatorConfig)
  }

  override def store(
      trafficBalance: TrafficBalance
  )(implicit traceContext: TraceContext): Future[Unit] = {
    batchAggregator.run(trafficBalance)
  }

  override def lookup(
      member: Member
  )(implicit traceContext: TraceContext): Future[Seq[TrafficBalance]] = {
    val query =
      sql"select member, sequencing_timestamp, balance, serial from sequencer_traffic_control_balance_updates where member = $member order by sequencing_timestamp asc"
    storage.query(query.as[TrafficBalance], functionFullName)
  }

  override def pruneBelowExclusive(
      member: Member,
      upToExclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    storage.update_(
      sqlu"""delete from sequencer_traffic_control_balance_updates where member = $member and sequencing_timestamp < (
        select max(sequencing_timestamp) from sequencer_traffic_control_balance_updates where member = $member and sequencing_timestamp <= $upToExclusive
      )""",
      functionFullName,
    )
  }
}
