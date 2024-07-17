// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.{
  DbDto,
  IngestionStorageBackend,
  ParameterStorageBackend,
}
import com.digitalasset.canton.platform.store.interning.StringInterning

import java.sql.Connection

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
      SQL"DELETE FROM lapi_command_completions WHERE ${queryStrategy
          .offsetIsGreater("completion_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_events_create WHERE ${queryStrategy.offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_events_consuming_exercise WHERE ${queryStrategy
          .offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_events_non_consuming_exercise WHERE ${queryStrategy
          .offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_events_unassign WHERE ${queryStrategy.offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_events_assign WHERE ${queryStrategy.offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_party_entries WHERE ${queryStrategy.offsetIsGreater("ledger_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_string_interning WHERE internal_id > $lastStringInterningId",
      SQL"DELETE FROM lapi_pe_create_id_filter_stakeholder WHERE event_sequential_id > $lastEventSequentialId",
      SQL"DELETE FROM lapi_pe_create_id_filter_non_stakeholder_informee WHERE event_sequential_id > $lastEventSequentialId",
      SQL"DELETE FROM lapi_pe_consuming_id_filter_stakeholder WHERE event_sequential_id > $lastEventSequentialId",
      SQL"DELETE FROM lapi_pe_consuming_id_filter_non_stakeholder_informee WHERE event_sequential_id > $lastEventSequentialId",
      SQL"DELETE FROM lapi_pe_non_consuming_id_filter_informee WHERE event_sequential_id > $lastEventSequentialId",
      SQL"DELETE FROM lapi_pe_unassign_id_filter_stakeholder WHERE event_sequential_id > $lastEventSequentialId",
      SQL"DELETE FROM lapi_pe_assign_id_filter_stakeholder WHERE event_sequential_id > $lastEventSequentialId",
      SQL"DELETE FROM lapi_transaction_meta WHERE ${queryStrategy
          .offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_transaction_metering WHERE ${queryStrategy
          .offsetIsGreater("ledger_offset", ledgerOffset)}",
      // As transfer global offsets are persisted before the ledger end, they might change after indexer recovery, so in the cleanup
      // phase here we make sure that all the persisted global offsets are revoked which are after the ledger end.
      SQL"UPDATE par_transfers SET transfer_out_global_offset = null WHERE transfer_out_global_offset > ${ledgerOffset.toLong}",
      SQL"UPDATE par_transfers SET transfer_in_global_offset = null WHERE transfer_in_global_offset > ${ledgerOffset.toLong}",
    ).map(_.execute()(connection)).discard
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
