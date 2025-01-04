// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.SubmissionTrackerStore
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.DbPrunableByTimeDomain
import com.digitalasset.canton.store.{IndexedDomain, PrunableByTimeParameters}
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.actionBasedSQLInterpolationCanton

import scala.concurrent.{ExecutionContext, Future}

class DbSubmissionTrackerStore(
    override protected val storage: DbStorage,
    override val indexedDomain: IndexedDomain,
    batchingParametersConfig: PrunableByTimeParameters,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends SubmissionTrackerStore
    with DbPrunableByTimeDomain
    with DbStore {

  override protected def batchingParameters: Option[PrunableByTimeParameters] = Some(
    batchingParametersConfig
  )

  override def registerFreshRequest(
      rootHash: RootHash,
      requestId: RequestId,
      maxSequencingTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = {
    // TODO(i13492): use batching to optimize processing

    val dbRequestId = requestId.unwrap

    val insertQuery =
      sqlu"""insert into par_fresh_submitted_transaction(
                 synchronizer_idx,
                 root_hash_hex,
                 request_id,
                 max_sequencing_time)
             values ($indexedDomain, $rootHash, $dbRequestId, $maxSequencingTime)
             on conflict do nothing"""

    val selectQuery =
      sql"""select count(*)
              from par_fresh_submitted_transaction
              where synchronizer_idx=$indexedDomain and root_hash_hex=$rootHash and request_id=$dbRequestId"""
        .as[Int]
        .headOption

    val f = for {
      _nbRows <- storage.update(insertQuery, "check freshness of submitted transaction")
      count <- storage.query(selectQuery, "lookup submitted transaction")
    } yield count.getOrElse(0) > 0

    FutureUnlessShutdown.outcomeF(f)
  }

  override protected[this] def pruning_status_table: String =
    "par_fresh_submitted_transaction_pruning"

  override protected[canton] def doPrune(
      beforeAndIncluding: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Int] = {
    val deleteQuery =
      sqlu"""delete from par_fresh_submitted_transaction
             where synchronizer_idx = $indexedDomain and max_sequencing_time <= $beforeAndIncluding"""

    storage.queryAndUpdate(deleteQuery, "prune par_fresh_submitted_transaction")
  }

  override def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage.updateUnlessShutdown_(
      sqlu"""delete from par_fresh_submitted_transaction
             where synchronizer_idx = $indexedDomain""",
      "purge par_fresh_submitted_transaction",
    )

  override def size(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int] = {
    val selectQuery =
      sql"""select count(*)
          from par_fresh_submitted_transaction"""
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
      sqlu"""delete from par_fresh_submitted_transaction
         where synchronizer_idx = $indexedDomain and request_id >= $including"""

    storage.update_(deleteQuery, "cleanup par_fresh_submitted_transaction")
  }
}
