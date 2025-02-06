// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    schema: Schema[DbDto]
) extends IngestionStorageBackend[AppendOnlySchema.Batch] {

  override def deletePartiallyIngestedData(
      ledgerEnd: Option[ParameterStorageBackend.LedgerEnd]
  )(connection: Connection): Unit = {
    val ledgerOffset = ledgerEnd.map(_.lastOffset)
    val lastStringInterningIdO = ledgerEnd.map(_.lastStringInterningId)
    val lastEventSequentialId = ledgerEnd.map(_.lastEventSeqId)

    List(
      SQL"DELETE FROM lapi_command_completions WHERE ${QueryStrategy
          .offsetIsGreater("completion_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_events_create WHERE ${QueryStrategy.offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_events_consuming_exercise WHERE ${QueryStrategy
          .offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_events_non_consuming_exercise WHERE ${QueryStrategy
          .offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_events_unassign WHERE ${QueryStrategy.offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_events_assign WHERE ${QueryStrategy.offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_party_entries WHERE ${QueryStrategy.offsetIsGreater("ledger_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_events_party_to_participant WHERE ${QueryStrategy
          .offsetIsGreater("event_offset", ledgerOffset)}",
      lastStringInterningIdO match {
        case None => SQL"DELETE FROM lapi_string_interning"
        case Some(lastStringInterningId) =>
          SQL"DELETE FROM lapi_string_interning WHERE internal_id > $lastStringInterningId"
      },
      SQL"DELETE FROM lapi_pe_create_id_filter_stakeholder WHERE ${QueryStrategy
          .eventSeqIdIsGreater("event_sequential_id", lastEventSequentialId)}",
      SQL"DELETE FROM lapi_pe_create_id_filter_non_stakeholder_informee WHERE ${QueryStrategy
          .eventSeqIdIsGreater("event_sequential_id", lastEventSequentialId)}",
      SQL"DELETE FROM lapi_pe_consuming_id_filter_stakeholder WHERE ${QueryStrategy
          .eventSeqIdIsGreater("event_sequential_id", lastEventSequentialId)}",
      SQL"DELETE FROM lapi_pe_consuming_id_filter_non_stakeholder_informee WHERE ${QueryStrategy
          .eventSeqIdIsGreater("event_sequential_id", lastEventSequentialId)}",
      SQL"DELETE FROM lapi_pe_non_consuming_id_filter_informee WHERE ${QueryStrategy
          .eventSeqIdIsGreater("event_sequential_id", lastEventSequentialId)}",
      SQL"DELETE FROM lapi_pe_unassign_id_filter_stakeholder WHERE ${QueryStrategy
          .eventSeqIdIsGreater("event_sequential_id", lastEventSequentialId)}",
      SQL"DELETE FROM lapi_pe_assign_id_filter_stakeholder WHERE ${QueryStrategy
          .eventSeqIdIsGreater("event_sequential_id", lastEventSequentialId)}",
      SQL"DELETE FROM lapi_transaction_meta WHERE ${QueryStrategy
          .offsetIsGreater("event_offset", ledgerOffset)}",
      SQL"DELETE FROM lapi_transaction_metering WHERE ${QueryStrategy
          .offsetIsGreater("ledger_offset", ledgerOffset)}",
      // As reassignment global offsets are persisted before the ledger end, they might change after indexer recovery, so in the cleanup
      // phase here we make sure that all the persisted global offsets are revoked which are after the ledger end.
      SQL"UPDATE par_reassignments SET unassignment_global_offset = null WHERE ${QueryStrategy
          .offsetIsGreater("unassignment_global_offset", ledgerOffset)}",
      SQL"UPDATE par_reassignments SET assignment_global_offset = null WHERE ${QueryStrategy
          .offsetIsGreater("assignment_global_offset", ledgerOffset)}",
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
