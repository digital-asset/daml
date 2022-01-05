// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.{DbDto, IngestionStorageBackend, ParameterStorageBackend}
import com.daml.platform.store.interning.StringInterning

private[backend] class IngestionStorageBackendTemplate(schema: Schema[DbDto])
    extends IngestionStorageBackend[AppendOnlySchema.Batch] {

  override def deletePartiallyIngestedData(
      ledgerEnd: Option[ParameterStorageBackend.LedgerEnd]
  )(connection: Connection): Unit = {
    ledgerEnd.foreach { existingLedgerEnd =>
      val ledgerOffset = existingLedgerEnd.lastOffset.toHexString.toString
      val lastStringInterningId = existingLedgerEnd.lastStringInterningId
      val lastEventSequentialId = existingLedgerEnd.lastEventSeqId

      SQL"DELETE FROM configuration_entries WHERE ledger_offset > ${ledgerOffset}"
        .execute()(connection)
      SQL"DELETE FROM package_entries WHERE ledger_offset > ${ledgerOffset}"
        .execute()(connection)
      SQL"DELETE FROM packages WHERE ledger_offset > ${ledgerOffset}"
        .execute()(connection)
      SQL"DELETE FROM participant_command_completions WHERE completion_offset > ${ledgerOffset}"
        .execute()(connection)
      SQL"DELETE FROM participant_events_divulgence WHERE event_offset > ${ledgerOffset}"
        .execute()(connection)
      SQL"DELETE FROM participant_events_create WHERE event_offset > ${ledgerOffset}"
        .execute()(connection)
      SQL"DELETE FROM participant_events_consuming_exercise WHERE event_offset > ${ledgerOffset}"
        .execute()(connection)
      SQL"DELETE FROM participant_events_non_consuming_exercise WHERE event_offset > ${ledgerOffset}"
        .execute()(connection)
      SQL"DELETE FROM party_entries WHERE ledger_offset > ${ledgerOffset}"
        .execute()(connection)
      SQL"DELETE FROM string_interning WHERE internal_id > ${lastStringInterningId}"
        .execute()(connection)
      SQL"DELETE FROM participant_events_create_filter WHERE event_sequential_id > ${lastEventSequentialId}"
        .execute()(connection)

      ()
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
