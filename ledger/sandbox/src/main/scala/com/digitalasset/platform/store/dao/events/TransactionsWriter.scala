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

private[dao] object TransactionsWriter extends TransactionsWriter {

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

  def apply(
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

      // Could be empty due to a transaction containing exclusively transient contracts
      if (!contractBatches.isEmpty) {
        contractBatches.foreach(_.execute())
        val divulgence = blinding.globalDivulgence.filterKeys(contractBatches.nonTransient)
        val witnesses = Relation.mapKeys(divulgence)(c => LedgerString.assertFromString(c.coid))
        WitnessesTable.ForContracts.prepareBatchInsert(witnesses).foreach(_.execute())
      }

    }
  }

}

private[dao] trait TransactionsWriter {

  def apply(
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
  )(implicit connection: Connection): Unit

}
