// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import anorm.{BatchSql, Row, SimpleSql}
import com.daml.ledger.participant.state.v1.{
  CommittedTransaction,
  DivulgedContract,
  Offset,
  SubmitterInfo
}
import com.daml.ledger.{TransactionId, WorkflowId}
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.BlindingInfo
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.DbType

object TransactionsWriter {

  final class PreparedInsert private[TransactionsWriter] (
      insertEventsBatch: Option[BatchSql],
      updateArchivesBatch: Option[BatchSql],
      deleteContractsBatch: Option[BatchSql],
      insertContractsBatch: Option[BatchSql],
      deleteWitnessesBatch: Option[BatchSql],
      insertWitnessesBatch: Option[BatchSql],
  ) {
    def write(metrics: Metrics)(implicit connection: Connection): Unit = {
      import metrics.daml.index.db.storeTransactionDbMetrics

      val eventsBatch = insertEventsBatch.toList ++ updateArchivesBatch.toList

      Timed.value(storeTransactionDbMetrics.eventsBatch, eventsBatch.foreach(_.execute()))

      // Delete the witnesses of contracts that being removed first, to
      // respect the foreign key constraint of the underlying storage
      for (batch <- deleteWitnessesBatch)
        Timed.value(storeTransactionDbMetrics.deleteContractWitnessesBatch, batch.execute())

      for (batch <- deleteContractsBatch) {
        Timed.value(storeTransactionDbMetrics.deleteContractsBatch, batch.execute())
      }
      for (batch <- insertContractsBatch) {
        Timed.value(storeTransactionDbMetrics.insertContractsBatch, batch.execute())
      }

      // Insert the witnesses last to respect the foreign key constraint of the underlying storage.
      // Compute and insert new witnesses regardless of whether the current transaction adds new
      // contracts because it may be the case that we are only adding new witnesses to existing
      // contracts (e.g. via divulging a contract with fetch).
      for (batch <- insertWitnessesBatch) {
        Timed.value(storeTransactionDbMetrics.insertContractWitnessesBatch, batch.execute())
      }
    }
  }

}

private[dao] final class TransactionsWriter(
    dbType: DbType,
    metrics: Metrics,
    lfValueTranslation: LfValueTranslation,
) {

  private val contractsTable = ContractsTable(dbType)
  private val contractWitnessesTable = ContractWitnessesTable(dbType)

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

    val blinding = blindingInfo.getOrElse(Blinding.blind(transaction))

    val insertion =
      TransactionIndexingInfo.from(
        blinding,
        submitterInfo,
        workflowId,
        transactionId,
        ledgerEffectiveTime,
        offset,
        transaction,
        divulgedContracts,
      )

    val serializedInsertion =
      Timed.value(
        metrics.daml.index.db.storeTransactionDbMetrics.translationTimer,
        insertion.serialize(lfValueTranslation)
      )

    val (insertEvents, updateArchives) =
      EventsTable.toExecutables(serializedInsertion)

    val (insertContracts, deleteContracts) =
      contractsTable.toExecutables(serializedInsertion)

    val (deleteWitnesses, insertWitnesses) =
      contractWitnessesTable.toExecutables(serializedInsertion)

    new TransactionsWriter.PreparedInsert(
      insertEventsBatch = insertEvents,
      updateArchivesBatch = updateArchives,
      deleteContractsBatch = deleteContracts,
      insertContractsBatch = insertContracts,
      deleteWitnessesBatch = deleteWitnesses,
      insertWitnessesBatch = insertWitnesses,
    )

  }

  def prepareEventsDelete(endInclusive: Offset): SimpleSql[Row] =
    EventsTable.prepareEventsDelete(endInclusive)

}
