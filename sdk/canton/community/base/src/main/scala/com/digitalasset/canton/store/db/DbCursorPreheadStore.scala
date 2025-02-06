// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, Counter}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore, TransactionalStoreUpdate}
import com.digitalasset.canton.store.{CursorPrehead, CursorPreheadStore, IndexedSynchronizer}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

/** DB storage for a cursor prehead for a synchronizer
  *
  * @param cursorTable The table name to store the cursor prehead.
  *                    The table must define the following columns:
  *                    <ul>
  *                    <li>synchronizer_idx integer not null primary key</li>
  *                      <li>prehead_counter bigint not null</li>
  *                      <li>ts bigint not null</li>
  *                    </ul>
  * @param processingTime The metric to be used for DB queries
  */
class DbCursorPreheadStore[Discr](
    indexedSynchronizer: IndexedSynchronizer,
    override protected val storage: DbStorage,
    cursorTable: String,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override private[store] implicit val ec: ExecutionContext)
    extends CursorPreheadStore[Discr]
    with DbStore {
  import storage.api.*

  @nowarn("msg=match may not be exhaustive")
  override def prehead(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CursorPrehead[Discr]]] = {
    val preheadQuery =
      sql"""select prehead_counter, ts from #$cursorTable where synchronizer_idx = $indexedSynchronizer order by prehead_counter desc #${storage
          .limit(2)}"""
        .as[(Counter[Discr], CantonTimestamp)]
    storage.query(preheadQuery, functionFullName).map {
      case Seq() => None
      case (preheadCounter, preheadTimestamp) +: rest =>
        if (rest.nonEmpty)
          logger.warn(
            s"Found several preheads for $indexedSynchronizer in $cursorTable instead of at most one; using $preheadCounter as prehead"
          )
        Some(CursorPrehead(preheadCounter, preheadTimestamp))
    }
  }

  @VisibleForTesting
  override private[canton] def overridePreheadUnsafe(
      newPrehead: Option[CursorPrehead[Discr]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.info(s"Override prehead counter in $cursorTable to $newPrehead")
    newPrehead match {
      case None => delete()
      case Some(CursorPrehead(counter, timestamp)) =>
        val query = storage.profile match {
          case _: DbStorage.Profile.H2 =>
            sqlu"merge into #$cursorTable (synchronizer_idx, prehead_counter, ts) values ($indexedSynchronizer, $counter, $timestamp)"
          case _: DbStorage.Profile.Postgres =>
            sqlu"""insert into #$cursorTable (synchronizer_idx, prehead_counter, ts) values ($indexedSynchronizer, $counter, $timestamp)
                     on conflict (synchronizer_idx) do update set prehead_counter = $counter, ts = $timestamp"""
        }
        storage.update_(query, functionFullName)
    }
  }

  override def advancePreheadToTransactionalStoreUpdate(
      newPrehead: CursorPrehead[Discr]
  )(implicit traceContext: TraceContext): TransactionalStoreUpdate = {
    logger.debug(s"Advancing prehead in $cursorTable to $newPrehead")
    val CursorPrehead(counter, timestamp) = newPrehead
    val query = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""
          merge into #$cursorTable as cursor_table
          using dual
          on cursor_table.synchronizer_idx = $indexedSynchronizer
            when matched and cursor_table.prehead_counter < $counter
              then update set cursor_table.prehead_counter = $counter, cursor_table.ts = $timestamp
            when not matched then insert (synchronizer_idx, prehead_counter, ts) values ($indexedSynchronizer, $counter, $timestamp)
          """
      case _: DbStorage.Profile.Postgres =>
        sqlu"""
          insert into #$cursorTable as cursor_table (synchronizer_idx, prehead_counter, ts)
          values ($indexedSynchronizer, $counter, $timestamp)
          on conflict (synchronizer_idx) do
            update set prehead_counter = $counter, ts = $timestamp
            where cursor_table.prehead_counter < $counter
          """
    }
    new TransactionalStoreUpdate.DbTransactionalStoreUpdate(
      query,
      storage,
      loggerFactory,
    )
  }

  override def rewindPreheadTo(
      newPreheadO: Option[CursorPrehead[Discr]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.info(s"Rewinding prehead to $newPreheadO")
    newPreheadO match {
      case None => delete()
      case Some(CursorPrehead(counter, timestamp)) =>
        val query =
          sqlu"""
            update #$cursorTable
            set prehead_counter = $counter, ts = $timestamp
            where synchronizer_idx = $indexedSynchronizer and prehead_counter > $counter"""
        storage.update_(query, "rewind prehead")
    }
  }

  private[this] def delete()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage.update_(
      sqlu"""delete from #$cursorTable where synchronizer_idx = $indexedSynchronizer""",
      functionFullName,
    )
}
