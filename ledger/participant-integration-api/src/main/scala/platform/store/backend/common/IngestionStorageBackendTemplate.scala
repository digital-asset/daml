// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.{DbDto, IngestionStorageBackend, ParameterStorageBackend}
import com.daml.platform.store.interning.StringInterning

private[backend] class IngestionStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    schema: Schema[DbDto],
) extends IngestionStorageBackend[AppendOnlySchema.Batch] {

  override def deletePartiallyIngestedData(
      ledgerEnd: ParameterStorageBackend.LedgerEnd
  )(connection: Connection): Unit = {
    val ledgerOffset = ledgerEnd.lastOffset
    val lastStringInterningId = ledgerEnd.lastStringInterningId
    val lastEventSequentialId = ledgerEnd.lastEventSeqId

    List(
      SQL"DELETE FROM configuration_entries WHERE ${queryStrategy.offsetIsGreater("ledger_offset", ledgerOffset)}",
      SQL"DELETE FROM package_entries WHERE ${queryStrategy.offsetIsGreater("ledger_offset", ledgerOffset)}",
      SQL"DELETE FROM packages WHERE ${queryStrategy.offsetIsGreater("ledger_offset", ledgerOffset)}",
      SQL"DELETE FROM participant_command_completions WHERE ${queryStrategy
          .offsetIsGreater("completion_offset", ledgerOffset)}",
      SQL"DELETE FROM participant_events_divulgence WHERE ${queryStrategy
          .offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM participant_events_create WHERE ${queryStrategy.offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM participant_events_consuming_exercise WHERE ${queryStrategy
          .offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM participant_events_non_consuming_exercise WHERE ${queryStrategy
          .offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM party_entries WHERE ${queryStrategy.offsetIsGreater("ledger_offset", ledgerOffset)}",
      SQL"DELETE FROM string_interning WHERE internal_id > $lastStringInterningId",
      SQL"DELETE FROM pe_create_id_filter_stakeholder WHERE event_sequential_id > $lastEventSequentialId",
      SQL"DELETE FROM transaction_metering WHERE ${queryStrategy.offsetIsGreater("ledger_offset", ledgerOffset)}",
    ).map(_.execute()(connection))

    ()
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
