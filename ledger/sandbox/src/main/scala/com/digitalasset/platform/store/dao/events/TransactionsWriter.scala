// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import anorm.BatchSql
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.{ApplicationId, CommandId, EventId, TransactionId, WorkflowId}
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.BlindingInfo
import com.daml.platform.events.EventIdFormatter
import com.daml.platform.store.DbType

private[dao] final class TransactionsWriter(dbType: DbType) {

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
    case (nodeId, l: LookupByKey) if l.result.exists(toBeInserted) =>
      l.result.get -> disclosure(nodeId)
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
      insertions: Set[ContractId],
      deletions: Set[ContractId],
      transaction: Transaction,
      blinding: BlindingInfo,
  ): Option[BatchSql] = {
    val localDivulgence = divulgence(transaction, blinding.disclosure, insertions)
    val fullDivulgence = Relation.union(
      localDivulgence,
      blinding.globalDivulgence.filterKeys(!deletions.contains(_))
    )
    val insertWitnessesBatch = contractWitnessesTable.prepareBatchInsert(fullDivulgence)
    if (localDivulgence.nonEmpty) {
      assert(insertWitnessesBatch.nonEmpty, "No witness found for contracts marked for insertion")
    }
    insertWitnessesBatch
  }

  def write(
      applicationId: Option[ApplicationId],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      commandId: Option[CommandId],
      submitter: Option[Party],
      roots: Set[NodeId],
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: Transaction,
      divulgedContracts: Iterable[(ContractId, Contract)],
  )(implicit connection: Connection): Unit = {

    val eventBatches = EventsTable.prepareBatchInsert(
      applicationId = applicationId,
      workflowId = workflowId,
      transactionId = transactionId,
      commandId = commandId,
      submitter = submitter,
      roots = roots,
      ledgerEffectiveTime = ledgerEffectiveTime,
      offset = offset,
      transaction = transaction,
    )

    if (eventBatches.isEmpty) {

      // Nothing to persist, avoid hitting the underlying storage
      ()

    } else {

      // `disclosure` says which node within the current transaction are visible to which party
      // `globalDivulgence` includes:
      // - contracts created on another participant
      // - contracts created in a previous transaction that are divulged in the current transaction
      val blinding = Blinding.blind(transaction)

      val disclosureForFlatTransaction =
        computeDisclosureForFlatTransaction(transactionId, transaction)

      val disclosureForTransactionTree =
        computeDisclosureForTransactionTree(transactionId, transaction, blinding)

      // Remove witnesses for the flat transactions from the full disclosure
      // This minimizes the data we save and allows us to use the union of the
      // witnesses for flat transactions and its complement to filter parties
      // for the transactions tree stream
      val disclosureComplement =
        Relation.diff(disclosureForTransactionTree, disclosureForFlatTransaction)

      // Prepare batch inserts for flat transactions
      val flatTransactionWitnessesBatch =
        WitnessesTable.ForFlatTransactions.prepareBatchInsert(
          witnesses = disclosureForFlatTransaction,
        )

      // Prepare batch inserts for all witnesses except those for flat transactions
      val complementWitnessesBatch =
        WitnessesTable.Complement.prepareBatchInsert(
          witnesses = disclosureComplement,
        )

      eventBatches.foreach(_.execute())
      flatTransactionWitnessesBatch.foreach(_.execute())
      complementWitnessesBatch.foreach(_.execute())

      val contractBatches = contractsTable.prepareBatchInsert(
        ledgerEffectiveTime = ledgerEffectiveTime,
        transaction = transaction,
        divulgedContracts = divulgedContracts,
      )

      for ((deleted, deleteContractsBatch) <- contractBatches.deletions) {
        val deleteWitnessesBatch = contractWitnessesTable.prepareBatchDelete(deleted.toSeq)
        assert(deleteWitnessesBatch.nonEmpty, "No witness found for contracts marked for deletion")
        // Delete the witnesses first to respect the foreign key constraint of the underlying storage
        deleteWitnessesBatch.get.execute()
        deleteContractsBatch.execute()
      }

      for ((_, insertContractsBatch) <- contractBatches.insertions) {
        insertContractsBatch.execute()
      }

      // Insert the witnesses last to respect the foreign key constraint of the underlying storage.
      // Compute and insert new witnesses regardless of whether the current transaction adds new
      // contracts because it may be the case that we are only adding new witnesses to existing
      // contracts (e.g. via divulging a contract with fetch).
      val insertWitnessesBatch = prepareWitnessesBatch(
        insertions = contractBatches.insertions.fold(Set.empty[ContractId])(_._1),
        deletions = contractBatches.deletions.fold(Set.empty[ContractId])(_._1),
        transaction = transaction,
        blinding = blinding,
      )
      insertWitnessesBatch.foreach(_.execute())

    }
  }

}
