// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.store.SubmissionTrackerStore
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.db.DbPrunableByTimeDomain
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.actionBasedSQLInterpolationCanton

import scala.concurrent.{ExecutionContext, Future}

class DbSubmissionTrackerStore(
    override protected val storage: DbStorage,
    override val domainId: IndexedDomain,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends SubmissionTrackerStore
    with DbPrunableByTimeDomain
    with DbStore {
  override def registerFreshRequest(
      rootHash: RootHash,
      requestId: RequestId,
      maxSequencingTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = {
    // TODO(i13492): use batching to optimize processing

    val dbRequestId = requestId.unwrap

    val insertQuery = storage.profile match {
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
        sqlu"""insert into fresh_submitted_transaction(
                 domain_id,
                 root_hash_hex,
                 request_id,
                 max_sequencing_time)
               values ($domainId, $rootHash, $dbRequestId, $maxSequencingTime)
               on conflict do nothing"""

      case _: DbStorage.Profile.Oracle =>
        sqlu"""merge into fresh_submitted_transaction
                 using (
                   select
                     $domainId domain_id,
                     $rootHash root_hash_hex,
                     $dbRequestId request_id,
                     $maxSequencingTime max_sequencing_time
                   from dual
                 ) to_insert
                 on (fresh_submitted_transaction.domain_id = to_insert.domain_id
                     and fresh_submitted_transaction.root_hash_hex = to_insert.root_hash_hex)
                 when not matched then
                   insert (
                     domain_id,
                     root_hash_hex,
                     request_id,
                     max_sequencing_time
                   ) values (
                     to_insert.domain_id,
                     to_insert.root_hash_hex,
                     to_insert.request_id,
                     to_insert.max_sequencing_time
                   )
          """
    }

    val selectQuery =
      sql"""select count(*)
              from fresh_submitted_transaction
              where domain_id=$domainId and root_hash_hex=$rootHash and request_id=$dbRequestId"""
        .as[Int]
        .headOption

    val f = for {
      _nbRows <- storage.update(insertQuery, "check freshness of submitted transaction")
      count <- storage.query(selectQuery, "lookup submitted transaction")
    } yield count.getOrElse(0) > 0

    FutureUnlessShutdown.outcomeF(f)
  }

  override protected[this] def pruning_status_table: String = "fresh_submitted_transaction_pruning"

  override protected val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("submission-tracker-store")

  override protected[canton] def doPrune(
      beforeAndIncluding: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    processingTime.event {
      val deleteQuery =
        sqlu"""delete from fresh_submitted_transaction
           where domain_id = $domainId and max_sequencing_time <= $beforeAndIncluding"""

      storage.update_(deleteQuery, "prune fresh_submitted_transaction")
    }
  }

  override def size(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int] = {
    val selectQuery =
      sql"""select count(*)
          from fresh_submitted_transaction"""
        .as[Int]
        .headOption

    val f = for {
      count <- storage.query(selectQuery, "count number of entries")
    } yield count.getOrElse(0)

    FutureUnlessShutdown.outcomeF(f)
  }

  override def deleteSince(
      including: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val deleteQuery =
      sqlu"""delete from fresh_submitted_transaction
         where domain_id = $domainId and request_id >= $including"""

    storage.update_(deleteQuery, "cleanup fresh_submitted_transaction")
  }
}
