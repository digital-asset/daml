// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.{SQL, SqlQuery}
import com.daml.platform.store.backend.{DbDto, IngestionStorageBackend, ParameterStorageBackend}
import com.daml.platform.store.interning.StringInterning

private[backend] class IngestionStorageBackendTemplate(schema: Schema[DbDto])
    extends IngestionStorageBackend[AppendOnlySchema.Batch] {

  private val SQL_DELETE_OVERSPILL_ENTRIES: List[SqlQuery] =
    List(
      SQL("DELETE FROM configuration_entries WHERE ledger_offset > {ledger_offset}"),
      SQL("DELETE FROM package_entries WHERE ledger_offset > {ledger_offset}"),
      SQL("DELETE FROM packages WHERE ledger_offset > {ledger_offset}"),
      SQL("DELETE FROM participant_command_completions WHERE completion_offset > {ledger_offset}"),
      SQL("DELETE FROM participant_events_divulgence WHERE event_offset > {ledger_offset}"),
      SQL("DELETE FROM participant_events_create WHERE event_offset > {ledger_offset}"),
      SQL("DELETE FROM participant_events_consuming_exercise WHERE event_offset > {ledger_offset}"),
      SQL(
        "DELETE FROM participant_events_non_consuming_exercise WHERE event_offset > {ledger_offset}"
      ),
      SQL("DELETE FROM party_entries WHERE ledger_offset > {ledger_offset}"),
      SQL("DELETE FROM string_interning WHERE internal_id > {last_string_interning_id}"),
    )

  override def deletePartiallyIngestedData(
      ledgerEnd: Option[ParameterStorageBackend.LedgerEnd]
  )(connection: Connection): Unit = {
    ledgerEnd.foreach { existingLedgerEnd =>
      SQL_DELETE_OVERSPILL_ENTRIES.foreach { query =>
        import com.daml.platform.store.Conversions.OffsetToStatement
        query
          .on("ledger_offset" -> existingLedgerEnd.lastOffset)
          .on("last_string_interning_id" -> existingLedgerEnd.lastStringInterningId)
          .execute()(connection)
        ()
      }
    }
  }

  override def insertBatch(
      connection: Connection,
      dbBatch: AppendOnlySchema.Batch,
  ): Unit =
    schema.executeUpdate(dbBatch, connection)

  override def batch(
      dbDtos: Vector[DbDto],
      stringInterning: StringInterning,
  ): AppendOnlySchema.Batch =
    schema.prepareData(dbDtos, stringInterning)
}
