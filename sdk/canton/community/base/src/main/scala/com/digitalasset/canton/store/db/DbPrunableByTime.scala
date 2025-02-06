// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.{IndexedString, IndexedSynchronizer, PrunableByTime}
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.SetParameter

import scala.concurrent.ExecutionContext

/** Mixin for a db store that stores the latest point in time when
  * pruning has started or finished.
  *
  * The pruning method of the store must use [[advancePruningTimestamp]] to signal the start end completion
  * of each pruning.
  */
trait DbPrunableByTime extends PrunableByTime {
  this: DbStore =>

  protected[this] implicit def setParameterIndexedSynchronizer: SetParameter[IndexedSynchronizer] =
    IndexedString.setParameterIndexedString

  /** The table name to store the pruning timestamp in.
    * The table must define the following fields:
    * <ul>
    *   <li>[[partitionColumn]] primary key</li>
    *   <li>`phase` stores the [[com.digitalasset.canton.pruning.PruningPhase]]</li>
    *   <li>`ts` stores the [[com.digitalasset.canton.data.CantonTimestamp]]<li>
    * </ul>
    */
  protected[this] def pruning_status_table: String

  protected[this] def partitionColumn: String = "synchronizer_idx"

  protected[this] def partitionKey: IndexedSynchronizer

  protected[this] implicit val ec: ExecutionContext

  import storage.api.*

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PruningStatus]] = {
    val query = sql"""
        select phase, ts, succeeded from #$pruning_status_table
        where #$partitionColumn = $partitionKey
        """.as[PruningStatus].headOption
    storage.query(query, functionFullName)
  }

  protected[canton] def advancePruningTimestamp(phase: PruningPhase, timestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {

    val query = (storage.profile, phase) match {
      case (_: DbStorage.Profile.Postgres, PruningPhase.Completed) =>
        sqlu"""
          UPDATE #$pruning_status_table SET phase = CAST($phase as pruning_phase), succeeded = $timestamp
          WHERE #$partitionColumn = $partitionKey AND ts = $timestamp
        """
      case (_, PruningPhase.Completed) =>
        sqlu"""
          UPDATE #$pruning_status_table SET phase = $phase, succeeded = $timestamp
          WHERE #$partitionColumn = $partitionKey AND ts = $timestamp
        """
      case (_: DbStorage.Profile.H2, PruningPhase.Started) =>
        sqlu"""
          merge into #$pruning_status_table as pruning_status
          using dual
          on pruning_status.#$partitionColumn = $partitionKey
            when matched and (pruning_status.ts < $timestamp)
              then update set pruning_status.phase = $phase, pruning_status.ts = $timestamp
            when not matched then insert (#$partitionColumn, phase, ts) values ($partitionKey, $phase, $timestamp)
          """
      case (_: DbStorage.Profile.Postgres, PruningPhase.Started) =>
        sqlu"""
          insert into #$pruning_status_table as pruning_status (#$partitionColumn, phase, ts)
          values ($partitionKey, CAST($phase as pruning_phase), $timestamp)
          on conflict (#$partitionColumn) do
            update set phase = CAST($phase as pruning_phase), ts = $timestamp
            where pruning_status.ts < $timestamp
          """
    }

    logger.debug(
      s"About to set phase of $pruning_status_table to \"${phase.kind}\" and timestamp to $timestamp"
    )

    for {
      rowCount <- storage.update(query, "pruning status upsert")
      _ <-
        if (logger.underlying.isDebugEnabled && rowCount != 1 && phase == PruningPhase.Started) {
          pruningStatus.map {
            case Some(previous) if previous.timestamp > timestamp =>
              logger.debug(
                s"Pruning at $timestamp started after another later pruning at ${previous.timestamp}."
              )
            case _ =>
          }
        } else FutureUnlessShutdown.pure(())
    } yield {
      logger.debug(
        s"Finished setting phase of $pruning_status_table to \"${phase.kind}\" and timestamp to $timestamp"
      )
    }
  }
}

/** Specialized [[DbPrunableByTime]] that uses the synchronizer as discriminator */
trait DbPrunableByTimeSynchronizer extends DbPrunableByTime {
  this: DbStore =>

  protected[this] def indexedSynchronizer: IndexedSynchronizer

  override protected[this] def partitionKey: IndexedSynchronizer = indexedSynchronizer

}
