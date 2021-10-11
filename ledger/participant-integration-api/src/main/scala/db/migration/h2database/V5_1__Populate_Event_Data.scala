// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Note: package name must correspond exactly to the flyway 'locations' setting, which defaults to
// 'db.migration.h2database' for h2database migrations
package com.daml.platform.db.migration.h2database

import java.sql.Connection

import anorm.{BatchSql, NamedParameter}
import com.daml.lf.data.Ref.LedgerString
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.{Transaction => Tx}
import com.daml.lf.transaction.Node.NodeCreate
import com.daml.platform.db.migration.translation.TransactionSerializer
import com.daml.platform.store.Conversions._
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

private[migration] class V5_1__Populate_Event_Data extends BaseJavaMigration {

  val SELECT_TRANSACTIONS =
    "select distinct le.transaction_id, le.transaction from contracts c join ledger_entries le  on c.transaction_id = le.transaction_id"

  def loadTransactions(conn: Connection) = {
    val statement = conn.createStatement()
    val rows = statement.executeQuery(SELECT_TRANSACTIONS)

    new Iterator[(LedgerString, Tx.Transaction)] {
      var hasNext: Boolean = rows.next()

      def next(): (LedgerString, Tx.Transaction) = {
        val transactionId = LedgerString.assertFromString(rows.getString("transaction_id"))
        val transaction = TransactionSerializer
          .deserializeTransaction(transactionId, rows.getBinaryStream("transaction"))
          .getOrElse(sys.error(s"failed to deserialize transaction $transactionId"))

        hasNext = rows.next()
        if (!hasNext) {
          statement.close()
        }

        transactionId -> transaction
      }
    }
  }

  private val batchSize = 10 * 1000

  override def migrate(context: Context): Unit = {
    val conn = context.getConnection

    val txs = loadTransactions(conn)
    val data = txs.flatMap { case (txId, tx) =>
      tx.nodes.collect { case (nodeId, NodeCreate(cid, _, _, _, signatories, stakeholders, _, _)) =>
        (cid, EventId(txId, nodeId), signatories, stakeholders -- signatories)
      }
    }

    data.grouped(batchSize).foreach { batch =>
      val updateContractsParams = batch.map { case (cid, eventId, _, _) =>
        Seq[NamedParameter]("event_id" -> eventId, "contract_id" -> cid.coid)
      }
      BatchSql(
        "UPDATE contracts SET create_event_id = {event_id} where id = {contract_id}",
        updateContractsParams.head,
        updateContractsParams.tail: _*
      ).execute()(conn)

      val signatories = batch.flatMap { case (cid, _, signatories, _) =>
        signatories
          .map(signatory => Seq[NamedParameter]("contract_id" -> cid.coid, "party" -> signatory))
      }
      BatchSql(
        "INSERT INTO contract_signatories VALUES ({contract_id}, {party})",
        signatories.head,
        signatories.tail: _*
      ).execute()(conn)

      val observers = batch.flatMap { case (cid, _, _, observers) =>
        observers
          .map(observer => Seq[NamedParameter]("contract_id" -> cid.coid, "party" -> observer))
      }
      if (observers.nonEmpty) {
        BatchSql(
          "INSERT INTO contract_observers VALUES ({contract_id}, {party})",
          observers.head,
          observers.tail: _*
        ).execute()(conn)
      }
      ()
    }
    ()
  }
}
