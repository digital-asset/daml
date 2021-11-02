// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Note: package name must correspond exactly to the flyway 'locations' setting, which defaults to
// 'db.migration.postgres' for postgres migrations
package com.daml.platform.db.migration.postgres

import java.sql.{Connection, ResultSet}

import anorm.{BatchSql, NamedParameter}
import com.daml.lf.data.Ref
import com.daml.lf.transaction.VersionedTransaction
import com.daml.lf.transaction.Node.{
  NodeRollback,
  NodeCreate,
  NodeExercises,
  NodeFetch,
  NodeLookupByKey,
}
import com.daml.platform.store.Conversions._
import com.daml.platform.db.migration.translation.TransactionSerializer
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

import scala.collection.compat.immutable.LazyList

private[migration] class V4_1__Collect_Parties extends BaseJavaMigration {

  // the number of contracts proceeded in a batch.
  private val batchSize = 10 * 1000

  def migrate(context: Context): Unit = {
    implicit val conn: Connection = context.getConnection
    updateParties(loadTransactions)
  }

  private def loadTransactions(implicit
      connection: Connection
  ): Iterator[(Long, VersionedTransaction)] = {

    val SQL_SELECT_LEDGER_ENTRIES =
      """SELECT
      |  ledger_offset,
      |  transaction
      |FROM
      |  ledger_entries
      |WHERE
      |  typ='transaction'""".stripMargin

    val statement = connection.createStatement()
    statement.setFetchSize(batchSize)
    val rows: ResultSet = statement.executeQuery(SQL_SELECT_LEDGER_ENTRIES)

    new Iterator[(Long, VersionedTransaction)] {

      var hasNext: Boolean = rows.next()

      def next(): (Long, VersionedTransaction) = {
        val ledgerOffset = rows.getLong("ledger_offset")
        val transactionId = Ref.LedgerString.assertFromString(rows.getString("transaction_id"))
        val transaction = TransactionSerializer
          .deserializeTransaction(transactionId, rows.getBinaryStream("transaction"))
          .getOrElse(
            sys.error(s"failed to deserialize transaction with ledger offset $ledgerOffset")
          )

        hasNext = rows.next()

        ledgerOffset -> transaction
      }
    }

  }

  private def updateParties(
      transactions: Iterator[(Long, VersionedTransaction)]
  )(implicit conn: Connection): Unit = {

    val SQL_INSERT_PARTY =
      """INSERT INTO
        |  parties(party, explicit, ledger_offset)
        |VALUES
        |  ({name}, false, {ledger_offset})
        |ON CONFLICT
        |  (party)
        |DO NOTHING
        |""".stripMargin

    val statements = transactions
      .flatMap { case (ledgerOffset, transaction) =>
        getParties(transaction).map(p => ledgerOffset -> p)
      }
      .map { case (ledgerOffset, name) =>
        Seq[NamedParameter]("name" -> name, "ledger_offset" -> ledgerOffset)
      }

    statements.to(LazyList).grouped(batchSize).foreach { batch =>
      BatchSql(
        SQL_INSERT_PARTY,
        batch.head,
        batch.tail: _*
      ).execute()
    }
  }

  private def getParties(transaction: VersionedTransaction): Set[Ref.Party] = {
    transaction
      .fold[Set[Ref.Party]](Set.empty) { case (parties, (_, node)) =>
        node match {
          case _: NodeRollback => Set.empty
          case nf: NodeFetch =>
            parties
              .union(nf.signatories)
              .union(nf.stakeholders)
              .union(nf.actingParties)
          case nc: NodeCreate =>
            parties
              .union(nc.signatories)
              .union(nc.stakeholders)
          case ne: NodeExercises =>
            parties
              .union(ne.signatories)
              .union(ne.stakeholders)
              .union(ne.actingParties)
          case _: NodeLookupByKey =>
            parties
        }
      }
  }
}
