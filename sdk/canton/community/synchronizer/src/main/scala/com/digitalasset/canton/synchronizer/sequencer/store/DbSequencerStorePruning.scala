// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.ExecutionContext

trait DbSequencerStorePruning {
  this: NamedLogging & DbStore =>

  protected val batchingConfig: BatchingConfig

  import storage.api.*

  protected def pruningIntervalsDBIO(
      table: String,
      indexedTimestampColumn: String,
      upperBound: CantonTimestamp,
  )(implicit
      ec: ExecutionContext
  ): DBIOAction[Seq[(CantonTimestamp, CantonTimestamp)], NoStream, Effect.Read] =
    sql"select min(#$indexedTimestampColumn) from #$table"
      .as[CantonTimestamp]
      .headOption
      .map(
        PruningUtils.pruningTimeIntervals(
          _,
          upperBound,
          batchingConfig.maxPruningTimeInterval.toInternal,
        )
      )

  protected def pruneIntervalsInBatches(
      intervals: Seq[(CantonTimestamp, CantonTimestamp)],
      table: String,
      indexedTimestampColumn: String,
      functionName: String = functionFullName,
  )(implicit ec: ExecutionContext, tc: TraceContext): FutureUnlessShutdown[Seq[Int]] =
    MonadUtil.batchedSequentialTraverse(
      batchingConfig.pruningParallelism,
      batchingConfig.maxItemsInBatch,
    )(intervals) { intervalsChunk =>
      val chunkDBIO = DbStorage.bulkOperation(
        s"delete from $table where $indexedTimestampColumn >= ? and $indexedTimestampColumn < ?",
        intervalsChunk,
        storage.profile,
        transactional = false,
      ) { pp => inverval =>
        val (from, upToExclusive) = inverval
        pp >> from
        pp >> upToExclusive
      }
      storage
        .queryAndUpdate(
          chunkDBIO,
          s"$functionName-$table",
        )
        .map(_.toSeq)
    }
}
