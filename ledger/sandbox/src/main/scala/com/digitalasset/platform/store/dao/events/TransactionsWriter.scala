// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import anorm.BatchSql
import com.daml.ledger.participant.state.v1.{DivulgedContract, Offset, SubmitterInfo}
import com.daml.ledger.{EventId, TransactionId, WorkflowId}
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.BlindingInfo
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.events.EventIdFormatter
import com.daml.platform.store.DbType

private[dao] object TransactionsWriter {

  final class PreparedInsert private[TransactionsWriter] (
      eventBatches: EventsTable.PreparedBatches,
      flatTransactionWitnessesBatch: Option[BatchSql],
      complementWitnessesBatch: Option[BatchSql],
      contractBatches: ContractsTable#PreparedBatches,
      deleteWitnessesBatch: Option[BatchSql],
      insertWitnessesBatch: Option[BatchSql],
  ) {
    def write()(implicit connection: Connection): Unit = {
      eventBatches.foreach(_.execute())
      flatTransactionWitnessesBatch.foreach(_.execute())
      complementWitnessesBatch.foreach(_.execute())

      // Delete the witnesses of contracts that being removed first, to
      // respect the foreign key constraint of the underlying storage
      deleteWitnessesBatch.map(_.execute())
      for ((_, deleteContractsBatch) <- contractBatches.deletions) {
        deleteContractsBatch.execute()
      }
      for ((_, insertContractsBatch) <- contractBatches.insertions) {
        insertContractsBatch.execute()
      }

      // Insert the witnesses last to respect the foreign key constraint of the underlying storage.
      // Compute and insert new witnesses regardless of whether the current transaction adds new
      // contracts because it may be the case that we are only adding new witnesses to existing
      // contracts (e.g. via divulging a contract with fetch).
      insertWitnessesBatch.foreach(_.execute())
    }
  }

}

private[dao] final class TransactionsWriter(
    dbType: DbType,
    metrics: Metrics,
) {

  private val contractsTable = ContractsTable(dbType)
  private val contractWitnessesTable = WitnessesTable.ForContracts(dbType)

  private def computeDisclosureForFlatTransaction(
      transactionId: TransactionId,
      transaction: Transaction,
  ): WitnessRelation[EventId] =
    transaction.nodes.collect {
      case (nodeId, c: Create) =>
        EventIdFormatter.fromTransactionId(transactionId, nodeId) -> c.stakeholders
      case (nodeId, e: Exercise) if e.consuming =>
        EventIdFormatter.fromTransactionId(transactionId, nodeId) -> e.stakeholders
    }

  private def computeDisclosureForTransactionTree(
      transactionId: TransactionId,
      transaction: Transaction,
      blinding: BlindingInfo,
  ): WitnessRelation[EventId] = {
    val disclosed =
      transaction.nodes.collect {
        case p @ (_, _: Create) => p
        case p @ (_, _: Exercise) => p
      }.keySet
    blinding.disclosure.collect {
      case (nodeId, party) if disclosed(nodeId) =>
        EventIdFormatter.fromTransactionId(transactionId, nodeId) -> party
    }
  }

  private def divulgedContracts(
      disclosure: DisclosureRelation,
      toBeInserted: Set[ContractId],
  ): PartialFunction[(NodeId, Node), (ContractId, Set[Party])] = {
    case (nodeId, c: Create) if toBeInserted(c.coid) =>
      c.coid -> disclosure(nodeId)
    case (nodeId, e: Exercise) if toBeInserted(e.targetCoid) =>
      e.targetCoid -> disclosure(nodeId)
    case (nodeId, f: Fetch) if toBeInserted(f.coid) =>
      f.coid -> disclosure(nodeId)
  }

  private def divulgence(
      transaction: Transaction,
      disclosure: DisclosureRelation,
      toBeInserted: Set[ContractId],
  ): WitnessRelation[ContractId] =
    if (toBeInserted.isEmpty) {
      Map.empty
    } else {
      transaction.nodes.iterator
        .collect(divulgedContracts(disclosure, toBeInserted))
        .foldLeft[WitnessRelation[ContractId]](Map.empty)(Relation.merge)
    }

  private def prepareWitnessesBatch(
      toBeInserted: Set[ContractId],
      toBeDeleted: Set[ContractId],
      transient: Set[ContractId],
      transaction: Transaction,
      blinding: BlindingInfo,
  ): Option[BatchSql] = {
    val localDivulgence = divulgence(transaction, blinding.disclosure, toBeInserted)
    val fullDivulgence = Relation.union(
      localDivulgence,
      blinding.globalDivulgence.filterKeys(cid => !toBeDeleted(cid) && !transient(cid))
    )
    val insertWitnessesBatch = contractWitnessesTable.prepareBatchInsert(fullDivulgence)
    if (localDivulgence.nonEmpty) {
      assert(insertWitnessesBatch.nonEmpty, "No witness found for contracts marked for insertion")
    }
    insertWitnessesBatch
  }

  def prepare(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: Transaction,
      divulgedContracts: Iterable[DivulgedContract],
  ): TransactionsWriter.PreparedInsert = {

    val rawEventBatches = EventsTable.prepareBatchInsert(
      submitterInfo = submitterInfo,
      workflowId = workflowId,
      transactionId = transactionId,
      ledgerEffectiveTime = ledgerEffectiveTime,
      offset = offset,
      transaction = transaction,
    )

    val blinding = Blinding.blind(transaction)

    val disclosureForFlatTransaction =
      computeDisclosureForFlatTransaction(transactionId, transaction)

    val disclosureForTransactionTree =
      computeDisclosureForTransactionTree(transactionId, transaction, blinding)

    // Prepare batch inserts for flat transactions
    val flatTransactionWitnessesBatch =
      WitnessesTable.ForFlatTransactions.prepareBatchInsert(
        witnesses = disclosureForFlatTransaction,
      )

    // Prepare batch inserts for all witnesses except those for flat transactions
    val complementWitnessesBatch =
      WitnessesTable.ForTransactionTrees.prepareBatchInsert(
        witnesses = disclosureForTransactionTree,
      )

    val rawContractBatches = contractsTable.prepareBatchInsert(
      ledgerEffectiveTime = ledgerEffectiveTime,
      transaction = transaction,
      divulgedContracts = divulgedContracts,
    )

    val deleteWitnessesBatch =
      for ((deleted, _) <- rawContractBatches.deletions) yield {
        val deletedWitnessesBatch = contractWitnessesTable.prepareBatchDelete(deleted.toSeq)
        deletedWitnessesBatch.getOrElse(
          throw new IllegalArgumentException("No witness found for contracts marked for deletion")
        )
      }

    val insertWitnessesBatch = prepareWitnessesBatch(
      toBeInserted = rawContractBatches.insertions.fold(Set.empty[ContractId])(_._1),
      toBeDeleted = rawContractBatches.deletions.fold(Set.empty[ContractId])(_._1),
      transient = rawContractBatches.transientContracts,
      transaction = transaction,
      blinding = blinding,
    )

    val (serializedEventBatches, serializedContractBatches) =
      Timed.value(
        metrics.daml.index.db.storeTransactionDao.translationTimer,
        (rawEventBatches.applySerialization(), rawContractBatches.applySerialization())
      )

    val eventBatches = serializedEventBatches.applyBatching()
    val contractBatches = serializedContractBatches.applyBatching()

    new TransactionsWriter.PreparedInsert(
      eventBatches = eventBatches,
      flatTransactionWitnessesBatch = flatTransactionWitnessesBatch,
      complementWitnessesBatch = complementWitnessesBatch,
      contractBatches = contractBatches,
      deleteWitnessesBatch = deleteWitnessesBatch,
      insertWitnessesBatch = insertWitnessesBatch,
    )

  }

}
