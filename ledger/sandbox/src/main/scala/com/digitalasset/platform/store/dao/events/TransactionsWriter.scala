// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.{ApplicationId, CommandId, EventId, TransactionId, WorkflowId}
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.BlindingInfo
import com.daml.platform.events.EventIdFormatter

private[dao] final class TransactionsWriter {

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
  ): PartialFunction[(NodeId, Node), (ContractId, Set[Party])] = {
    case (nodeId, c: Create) =>
      for (d <- disclosure(nodeId)) {
        println(s"create: divulging ${c.coid} to $d")
      }
      c.coid -> disclosure(nodeId)
    case (nodeId, e: Exercise) =>
      for (d <- disclosure(nodeId)) {
        println(s"exercise: divulging ${e.targetCoid} to $d")
      }
      e.targetCoid -> disclosure(nodeId)
    case (nodeId, f: Fetch) =>
      for (d <- disclosure(nodeId)) {
        println(s"fetch: divulging ${f.coid} to $d")
      }
      f.coid -> disclosure(nodeId)
    case (nodeId, l: LookupByKey) if l.result.isDefined =>
      for (d <- disclosure(nodeId)) {
        println(s"lookup-by-key: divulging ${l.result.get} to $d")
      }
      l.result.get -> disclosure(nodeId)
  }

  private def divulgence(
      transaction: Transaction,
      disclosure: DisclosureRelation,
  ): WitnessRelation[ContractId] =
    transaction.nodes.iterator
      .collect(divulgedContracts(disclosure))
      .foldLeft[WitnessRelation[ContractId]](Map.empty)(Relation.merge)

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

      val contractBatches = ContractsTable.prepareBatchInsert(
        ledgerEffectiveTime = ledgerEffectiveTime,
        transaction = transaction,
        divulgedContracts = divulgedContracts,
      )

      for ((deleted, deleteContractsBatch) <- contractBatches.deletions) {
        val deleteWitnessesBatch = WitnessesTable.ForContracts.prepareBatchDelete(deleted.toSeq)
        require(deleteWitnessesBatch.nonEmpty, "Invalid empty batch of witnesses to delete")
        // Delete the witnesses first to respect the foreign key constraint of the underlying storage
        deleteWitnessesBatch.get.execute()
        deleteContractsBatch.execute()
      }

      for ((inserted, insertContractsBatch) <- contractBatches.insertions) {
        val localDivulgence = divulgence(transaction, blinding.disclosure).filterKeys(inserted)
        println(s"divulged (transactionWriter): ${blinding.globalDivulgence}")
        val fullDivulgence = Relation.union(localDivulgence, blinding.globalDivulgence)
        val insertWitnessesBatch = WitnessesTable.ForContracts.prepareBatchInsert(fullDivulgence)
        require(insertWitnessesBatch.nonEmpty, "Invalid empty batch of witnesses to insert")
        // Insert the witnesses last to respect the foreign key constraint of the underlying storage
        insertContractsBatch.execute()
        insertWitnessesBatch.get.execute()
      }

    }
  }

}
