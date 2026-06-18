// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import com.digitalasset.canton.data
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.backend.Conversions.*
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.protocol.UpdateId

import java.sql.Connection

class UpdatePointwiseQueries(
    ledgerEndCache: LedgerEndCache
) {
  import EventStorageBackendTemplate.*

  /** Fetches a matching event sequential id range. */
  def fetchIdsFromUpdateMeta(
      lookupKey: LookupKey
  )(connection: Connection): Option[(Long, Long)] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    // 1. Checking whether "event_offset <= ledgerEndOffset" is needed because during indexing
    // the events and transaction_meta tables are written to prior to the ledger end being updated.
    val ledgerEndOffsetO: Option[Offset] = ledgerEndCache().map(_.lastOffset)

    ledgerEndOffsetO.flatMap { ledgerEndOffset =>
      val lookupKeyClause: CompositeSql =
        lookupKey match {
          case LookupKey.ByUpdateId(updateId) =>
            cSQL"t.update_id = $updateId"
          case LookupKey.ByOffset(offset) =>
            cSQL"t.event_offset = $offset"
        }

      SQL"""
         SELECT
            t.event_sequential_id_first,
            t.event_sequential_id_last
         FROM
            lapi_update_meta t
         WHERE
            $lookupKeyClause
           AND
            t.event_offset <= $ledgerEndOffset
       """.as(EventSequentialIdFirstLast.singleOpt)(connection)
    }
  }
}

object UpdatePointwiseQueries {
  sealed trait LookupKey
  object LookupKey {
    final case class ByUpdateId(updateId: UpdateId) extends LookupKey
    final case class ByOffset(offset: data.Offset) extends LookupKey
  }
}
