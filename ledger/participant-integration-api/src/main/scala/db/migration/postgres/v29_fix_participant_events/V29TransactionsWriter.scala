// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.db.migration.postgres.v29_fix_participant_events

import java.sql.Connection
import java.time.Instant

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.engine.Blinding
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.BlindingInfo

private[migration] object V29TransactionsWriter extends V29TransactionsWriter {

  private def computeDisclosureForFlatTransaction(
      transactionId: Ref.TransactionId,
      transaction: Transaction,
  ): WitnessRelation[EventId] =
    transaction.unversioned.nodes.collect {
      case (nodeId, c: Create) =>
        EventId(transactionId, nodeId) -> c.stakeholders
      case (nodeId, e: Exercise) if e.consuming =>
        EventId(transactionId, nodeId) -> e.stakeholders
    }

  private def computeDisclosureForTransactionTree(
      transactionId: Ref.TransactionId,
      transaction: Transaction,
      blinding: BlindingInfo,
  ): WitnessRelation[EventId] = {
    val disclosed =
      transaction.unversioned.nodes.collect {
        case p @ (_, _: Create) => p
        case p @ (_, _: Exercise) => p
      }.keySet
    blinding.disclosure.collect {
      case (nodeId, party) if disclosed(nodeId) =>
        EventId(transactionId, nodeId) -> party
    }
  }

  def apply(
      applicationId: Option[Ref.ApplicationId],
      workflowId: Option[Ref.WorkflowId],
      transactionId: Ref.TransactionId,
      commandId: Option[Ref.CommandId],
      submitter: Option[Party],
      roots: Set[NodeId],
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: Transaction,
  )(implicit connection: Connection): Unit = {

    val eventBatches = V29EventsTableInsert.prepareBatchInsert(
      applicationId = applicationId,
      workflowId = workflowId,
      transactionId = transactionId,
      commandId = commandId,
      submitter = submitter,
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
        V29WitnessesTable.ForFlatTransactions.prepareBatchInsert(
          witnesses = disclosureForFlatTransaction
        )

      // Prepare batch inserts for all witnesses except those for flat transactions
      val complementWitnessesBatch =
        V29WitnessesTable.Complement.prepareBatchInsert(
          witnesses = disclosureComplement
        )

      eventBatches.foreach(_.execute())
      flatTransactionWitnessesBatch.foreach(_.execute())
      complementWitnessesBatch.foreach(_.execute())
    }
  }

}

private[migration] trait V29TransactionsWriter {

  def apply(
      applicationId: Option[Ref.ApplicationId],
      workflowId: Option[Ref.WorkflowId],
      transactionId: Ref.TransactionId,
      commandId: Option[Ref.CommandId],
      submitter: Option[Party],
      roots: Set[NodeId],
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: Transaction,
  )(implicit connection: Connection): Unit

}
