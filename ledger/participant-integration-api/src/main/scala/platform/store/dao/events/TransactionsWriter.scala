// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import anorm.{Row, SimpleSql}
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
      eventsTableExecutables: EventsTable.Executables,
      contractsTableExecutables: ContractsTable.Executables,
      contractWitnessesTableExecutables: ContractWitnessesTable.Executables,
  ) {
    def write(metrics: Metrics)(implicit connection: Connection): Unit = {
      import metrics.daml.index.db.storeTransactionDbMetrics

      val eventsBatch = eventsTableExecutables.insertEvents.toList ++ eventsTableExecutables.updateArchives.toList

      Timed.value(storeTransactionDbMetrics.eventsBatch, eventsBatch.foreach(_.execute()))

      // Delete the witnesses of contracts that being removed first, to
      // respect the foreign key constraint of the underlying storage
      for (inserts <- contractWitnessesTableExecutables.deleteWitnesses)
        Timed.value(storeTransactionDbMetrics.deleteContractWitnessesBatch, inserts.execute())

      for (deletes <- contractsTableExecutables.deleteContracts) {
        Timed.value(storeTransactionDbMetrics.deleteContractsBatch, deletes.execute())
      }

      for (inserts <- contractsTableExecutables.insertContracts) {
        Timed.value(storeTransactionDbMetrics.insertContractsBatch, inserts.execute())
      }

      // Insert the witnesses last to respect the foreign key constraint of the underlying storage.
      // Compute and insert new witnesses regardless of whether the current transaction adds new
      // contracts because it may be the case that we are only adding new witnesses to existing
      // contracts (e.g. via divulging a contract with fetch).
      for (inserts <- contractWitnessesTableExecutables.insertWitnesses) {
        Timed.value(storeTransactionDbMetrics.insertContractWitnessesBatch, inserts.execute())
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

    val transactionIndexingInfo =
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

    val serializedTransactionIndexingInfo =
      Timed.value(
        metrics.daml.index.db.storeTransactionDbMetrics.translationTimer,
        transactionIndexingInfo.serialize(lfValueTranslation),
      )

    new TransactionsWriter.PreparedInsert(
      EventsTable.toExecutables(serializedTransactionIndexingInfo),
      contractsTable.toExecutables(serializedTransactionIndexingInfo),
      contractWitnessesTable.toExecutables(serializedTransactionIndexingInfo),
    )

  }

  def prepareEventsDelete(endInclusive: Offset): SimpleSql[Row] =
    EventsTable.prepareEventsDelete(endInclusive)

}
