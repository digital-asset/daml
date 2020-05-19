// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Note: package name must correspond exactly to the flyway 'locations' setting, which defaults to
// 'db.migration.postgres' for postgres migrations
package db.migration.postgres

import java.sql.{Connection, ResultSet}

import anorm.{BatchSql, NamedParameter}
import com.daml.lf.data.Ref
import com.daml.lf.transaction.GenTransaction
import com.daml.lf.transaction.Node.{NodeCreate, NodeExercises, NodeFetch, NodeLookupByKey}
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.ledger.EventId
import com.daml.platform.store.Conversions._
import db.migration.translation.TransactionSerializer
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

class V4_1__Collect_Parties extends BaseJavaMigration {

  private type Transaction = GenTransaction.WithTxValue[EventId, AbsoluteContractId]

  // the number of contracts proceeded in a batch.
  private val batchSize = 10 * 1000

  def migrate(context: Context): Unit = {
    implicit val conn: Connection = context.getConnection
    updateParties(loadTransactions)
  }

  private def loadTransactions(
      implicit connection: Connection
  ): Iterator[(Long, Transaction)] = {

    val SQL_SELECT_LEDGER_ENTRIES =
      """SELECT
      |  ledger_offset,
      |  transaction
      |FROM
      |  ledger_entries
      |WHERE
      |  typ='transaction'""".stripMargin

    val rows: ResultSet = connection.createStatement().executeQuery(SQL_SELECT_LEDGER_ENTRIES)

    new Iterator[(Long, Transaction)] {

      var hasNext: Boolean = rows.next()

      def next(): (Long, Transaction) = {
        val ledgerOffset = rows.getLong("ledger_offset")
        val transaction = TransactionSerializer
          .deserializeTransaction(rows.getBinaryStream("transaction"))
          .getOrElse(
            sys.error(s"failed to deserialize transaction with ledger offset $ledgerOffset"))

        hasNext = rows.next()

        ledgerOffset -> transaction
      }
    }

  }

  private def updateParties(transactions: Iterator[(Long, Transaction)])(
      implicit conn: Connection): Unit = {

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
      .flatMap {
        case (ledgerOffset, transaction) =>
          getParties(transaction).map(p => ledgerOffset -> p)
      }
      .map {
        case (ledgerOffset, name) =>
          Seq[NamedParameter]("name" -> name, "ledger_offset" -> ledgerOffset)
      }

    statements.toStream.grouped(batchSize).foreach { batch =>
      BatchSql(
        SQL_INSERT_PARTY,
        batch.head,
        batch.tail: _*
      ).execute()
    }
  }

  private def getParties(transaction: Transaction): Set[Ref.Party] = {
    transaction
      .fold[Set[Ref.Party]](Set.empty) {
        case (parties, (_, node)) =>
          node match {
            case nf: NodeFetch.WithTxValue[AbsoluteContractId] =>
              parties
                .union(nf.signatories)
                .union(nf.stakeholders)
                .union(nf.actingParties.getOrElse(Set.empty))
            case nc: NodeCreate.WithTxValue[AbsoluteContractId] =>
              parties
                .union(nc.signatories)
                .union(nc.stakeholders)
            case ne: NodeExercises.WithTxValue[_, AbsoluteContractId] =>
              parties
                .union(ne.signatories)
                .union(ne.stakeholders)
                .union(ne.actingParties)
            case _: NodeLookupByKey.WithTxValue[AbsoluteContractId] =>
              parties
          }
      }
  }
}
