// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao.events

import java.util.Date

import com.digitalasset.ledger.{ApplicationId, CommandId, TransactionId, WorkflowId}
import com.digitalasset.platform.index.Disclosure
import com.digitalasset.platform.store.dao.{DbDispatcher, LedgerDao, PersistenceResponse}

import scala.concurrent.Future

private[dao] object TransactionWriter {

  def apply(dispatcher: DbDispatcher): TransactionWriter[LedgerDao#LedgerOffset] =
    (
        applicationId: Option[ApplicationId],
        workflowId: Option[WorkflowId],
        transactionId: TransactionId,
        commandId: Option[CommandId],
        submitter: Option[Party],
        roots: Set[NodeId],
        ledgerEffectiveTime: Date,
        offset: LedgerDao#LedgerOffset,
        transaction: Transaction,
    ) => {

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
        Future.successful(PersistenceResponse.Ok)

      } else {

        // Retrieve the disclosure for flat transaction and transaction trees
        val disclosureForFlatTransaction = Disclosure.forFlatTransaction(transaction)
        val disclosureForTransactionTree = Disclosure.forTransactionTree(transaction)

        // Remove witnesses for the flat transactions from the full disclosure
        // This minimizes the data we save and allows us to use the union of the
        // witnesses for flat transactions and its complement to filter parties
        // for the transactions tree stream
        val disclosureComplement =
          DisclosureRelation.diff(disclosureForTransactionTree, disclosureForFlatTransaction)

        // Prepare batch inserts for flat transactions
        val flatTransactionWitnessesBatch =
          WitnessesTable.ForFlatTransactions.prepareBatchInsert(
            offset = offset,
            transactionId = transactionId,
            witnesses = disclosureForFlatTransaction,
          )

        // Prepare batch inserts for all witnesses except those for flat transactions
        val complementWitnessesBatch =
          WitnessesTable.Complement.prepareBatchInsert(
            offset = offset,
            transactionId = transactionId,
            witnesses = disclosureComplement,
          )

        dispatcher.executeSql("store_transaction", Some(s"transaction: $transactionId")) {
          implicit connection =>
            {
              eventBatches.foreach(_.execute())
              flatTransactionWitnessesBatch.foreach(_.execute())
              complementWitnessesBatch.foreach(_.execute())
              PersistenceResponse.Ok
            }
        }
      }
    }

}

trait TransactionWriter[LedgerOffset] {

  def storeTransaction(
      applicationId: Option[ApplicationId],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      commandId: Option[CommandId],
      submitter: Option[Party],
      roots: Set[NodeId],
      ledgerEffectiveTime: Date,
      offset: LedgerOffset,
      transaction: Transaction,
  ): Future[PersistenceResponse]

}
