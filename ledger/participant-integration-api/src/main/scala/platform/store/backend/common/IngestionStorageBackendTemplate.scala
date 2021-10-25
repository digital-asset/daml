// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.{SQL, SqlQuery}
import com.daml.platform.store.backend.{IngestionStorageBackend, ParameterStorageBackend}

private[backend] trait IngestionStorageBackendTemplate[DB_BATCH]
    extends IngestionStorageBackend[DB_BATCH] {

  // List of (table name, name of column that stores the offset at which the row was inserted)
  private val tables: List[(String, String)] =
    List(
      "configuration_entries" -> "ledger_offset",
      "package_entries" -> "ledger_offset",
      "packages" -> "ledger_offset",
      "participant_command_completions" -> "completion_offset",
      "participant_events_divulgence" -> "event_offset",
      "participant_events_create" -> "event_offset",
      "participant_events_consuming_exercise" -> "event_offset",
      "participant_events_non_consuming_exercise" -> "event_offset",
      "party_entries" -> "ledger_offset",
    )

  private val SQL_DELETE_OVERSPILL_ENTRIES: List[SqlQuery] =
    tables.map { case (table, column) =>
      SQL(s"DELETE FROM $table WHERE $column > {ledger_offset}")
    }

  private val SQL_DELETE_ALL_ENTRIES: List[SqlQuery] =
    tables.map { case (table, _) => SQL(s"TRUNCATE TABLE $table") }

  override def deletePartiallyIngestedData(
      ledgerEnd: Option[ParameterStorageBackend.LedgerEnd]
  )(connection: Connection): Unit = {
    ledgerEnd match {
      case Some(existingLedgerEnd) =>
        SQL_DELETE_OVERSPILL_ENTRIES.foreach { query =>
          import com.daml.platform.store.Conversions.OffsetToStatement
          query
            .on("ledger_offset" -> existingLedgerEnd.lastOffset)
            .execute()(connection)
        }
      case None =>
        // There is no ledger end, but there can still be partially ingested data
        SQL_DELETE_ALL_ENTRIES.foreach { query =>
          query.execute()(connection)
        }
    }
    ()
  }
}
