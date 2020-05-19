// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.postgres

import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.platform.events.EventIdFormatter
import db.migration.postgres.v29_fix_participant_events.V29TransactionsWriter
import db.migration.translation.TransactionSerializer
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

class V29__Fix_participant_events extends BaseJavaMigration {

  val TRUNCATE_PARTICIPANT_EVENTS =
    "truncate table participant_events cascade"

  val SELECT_TRANSACTIONS =
    "select * from ledger_entries where typ='transaction' order by ledger_offset asc"

  override def migrate(context: Context): Unit = {
    val conn = context.getConnection

    val truncateEvents = conn.createStatement()
    truncateEvents.execute(TRUNCATE_PARTICIPANT_EVENTS)
    conn.commit()

    val loadTransactions = conn.createStatement()
    val rows = loadTransactions.executeQuery(SELECT_TRANSACTIONS)

    def getNonEmptyString(name: String): Option[String] =
      Option(rows.getString(name)).filter(s => !rows.wasNull() && s.nonEmpty)

    while (rows.next()) {
      val transactionId = Ref.LedgerString.assertFromString(rows.getString("transaction_id"))
      val applicationId =
        getNonEmptyString("application_id").map(Ref.LedgerString.assertFromString)
      val commandId = getNonEmptyString("command_id").map(Ref.LedgerString.assertFromString)
      val submitter = getNonEmptyString("submitter").map(Ref.Party.assertFromString)
      val workflowId = getNonEmptyString("workflow_id").map(Ref.LedgerString.assertFromString)
      val let = rows.getTimestamp("effective_at").toInstant
      val offset = Offset.fromByteArray(rows.getBytes("ledger_offset"))

      val transaction = TransactionSerializer
        .deserializeTransaction(rows.getBinaryStream("transaction"))
        .getOrElse(sys.error(s"failed to deserialize transaction $transactionId"))
        .mapNodeId(evId => EventIdFormatter.split(evId).map(_.nodeId).get)

      V29TransactionsWriter.apply(
        applicationId,
        workflowId,
        transactionId,
        commandId,
        submitter,
        transaction.roots.toSeq.toSet,
        let,
        offset,
        transaction,
      )(conn)
    }
    loadTransactions.close()
  }

}
