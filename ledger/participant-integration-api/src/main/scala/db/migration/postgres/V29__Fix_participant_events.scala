// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.db.migration.postgres

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.platform.db.migration.postgres.v29_fix_participant_events.V29TransactionsWriter
import com.daml.platform.db.migration.translation.TransactionSerializer
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

private[migration] class V29__Fix_participant_events extends BaseJavaMigration {

  val TRUNCATE_PARTICIPANT_EVENTS =
    "truncate table participant_events cascade"

  val SELECT_TRANSACTIONS =
    "select * from ledger_entries where typ='transaction' order by ledger_offset asc"

  val BATCH_SIZE = 500

  override def migrate(context: Context): Unit = {
    val conn = context.getConnection

    val truncateEvents = conn.createStatement()
    truncateEvents.execute(TRUNCATE_PARTICIPANT_EVENTS)
    conn.commit()

    val loadTransactions = conn.createStatement()
    loadTransactions.setFetchSize(BATCH_SIZE)
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
        .deserializeTransaction(transactionId, rows.getBinaryStream("transaction"))
        .getOrElse(sys.error(s"failed to deserialize transaction $transactionId"))

      V29TransactionsWriter.apply(
        applicationId,
        workflowId,
        transactionId,
        commandId,
        submitter,
        transaction.unversioned.roots.toSeq.toSet,
        let,
        offset,
        transaction,
      )(conn)
    }
    loadTransactions.close()
  }

}
