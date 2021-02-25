// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import anorm.{Row, SimpleSql}
import com.daml.ledger.participant.state.v1.{
  CommittedTransaction,
  DivulgedContract,
  Offset,
  SubmitterInfo,
}
import com.daml.ledger.{TransactionId, WorkflowId}
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.BlindingInfo
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.DbType

private[platform] object TransactionsWriter {
  private[platform] class PreparedInsert(eventsTableExecutables: EventsTable.Batches) {
    def write(metrics: Metrics)(implicit connection: Connection): Unit = {
      writeEvents(metrics)
    }

    def writeEvents(metrics: Metrics)(implicit connection: Connection): Unit =
      Timed.value(
        metrics.daml.index.db.storeTransactionDbMetrics.eventsBatch,
        eventsTableExecutables.execute(),
      )
  }
}

private[platform] final class TransactionsWriter(
    dbType: DbType,
    metrics: Metrics,
    lfValueTranslation: LfValueTranslation,
    compressionStrategy: CompressionStrategy,
    compressionMetrics: CompressionMetrics,
    idempotentEventInsertions: Boolean = false,
) {

  private val eventsTable = EventsTable(dbType, idempotentEventInsertions)

  def prepare(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: CommittedTransaction,
      divulgedContracts: Iterable[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
  ): TransactionsWriter.PreparedInsert = {

    // Backwards-compatibility: blinding info was previously not pre-computed and saved by the committer
    // but rather computed on the read path from Fetch and LookupByKey nodes (now trimmed just before
    // commit, for storage reduction).
    val blinding = blindingInfo.getOrElse(Blinding.blind(transaction))

    val indexing =
      TransactionIndexing.from(
        blinding,
        submitterInfo,
        workflowId,
        transactionId,
        ledgerEffectiveTime,
        offset,
        transaction,
        divulgedContracts,
      )

    val serialized =
      Timed.value(
        metrics.daml.index.db.storeTransactionDbMetrics.translationTimer,
        TransactionIndexing.serialize(
          lfValueTranslation,
          transactionId,
          indexing.events.events,
          divulgedContracts,
        ),
      )

    val compressed =
      Timed.value(
        metrics.daml.index.db.storeTransactionDbMetrics.compressionTimer,
        TransactionIndexing.compress(
          serialized,
          compressionStrategy,
          compressionMetrics,
        ),
      )

    new TransactionsWriter.PreparedInsert(
      eventsTable.toExecutables(indexing.transaction, indexing.events, compressed)
    )
  }

  def prepareEventsDelete(endInclusive: Offset): SimpleSql[Row] =
    EventsTableDelete.prepareEventsDelete(endInclusive)

}
