// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{bool, flatten, str}
import anorm.{RowParser, ~}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexerPartyDetails
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.backend.Conversions.{
  ledgerString,
  offset,
  party,
  timestampFromMicros,
}
import com.digitalasset.canton.platform.store.backend.PartyStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlAsVectorOf.*
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.dao.JdbcLedgerDao.{acceptType, rejectType}
import com.digitalasset.canton.platform.store.entries.PartyLedgerEntry

import java.sql.Connection

class PartyStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
) extends PartyStorageBackend {

  private val partyEntryParser: RowParser[(Offset, PartyLedgerEntry)] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.bigDecimalColumnToBoolean
    (offset("ledger_offset") ~
      timestampFromMicros("recorded_at") ~
      ledgerString("submission_id").? ~
      party("party").? ~
      str("display_name").? ~
      str("typ") ~
      str("rejection_reason").? ~
      bool("is_local").?)
      .map(flatten)
      .map {
        case (
              offset,
              recordTime,
              submissionIdOpt,
              Some(party),
              displayNameOpt,
              `acceptType`,
              None,
              Some(isLocal),
            ) =>
          offset ->
            PartyLedgerEntry.AllocationAccepted(
              submissionIdOpt,
              recordTime,
              IndexerPartyDetails(party, displayNameOpt, isLocal),
            )
        case (
              offset,
              recordTime,
              Some(submissionId),
              None,
              None,
              `rejectType`,
              Some(reason),
              None,
            ) =>
          offset -> PartyLedgerEntry.AllocationRejected(
            submissionId,
            recordTime,
            reason,
          )
        case invalidRow =>
          sys.error(s"getPartyEntries: invalid party entry row: $invalidRow")
      }
  }

  override def partyEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, PartyLedgerEntry)] = {
    SQL"""select * from lapi_party_entries
      where ${queryStrategy.offsetIsBetween(
        nonNullableColumn = "ledger_offset",
        startExclusive = startExclusive,
        endInclusive = endInclusive,
      )}
      order by ledger_offset asc
      offset $queryOffset rows
      fetch next $pageSize rows only
      """
      .asVectorOf(partyEntryParser)(connection)
  }

  private val partyDetailsParser: RowParser[IndexerPartyDetails] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.bigDecimalColumnToBoolean
    str("party") ~
      str("display_name").? ~
      bool("is_local") map { case party ~ displayName ~ isLocal =>
        IndexerPartyDetails(
          party = Party.assertFromString(party),
          displayName = displayName.filter(_.nonEmpty),
          isLocal = isLocal,
        )
      }
  }

  private def queryParties(
      parties: Option[Set[String]],
      connection: Connection,
  ): Vector[IndexerPartyDetails] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    val partyFilter = parties match {
      case Some(requestedParties) => cSQL"lapi_party_entries.party in ($requestedParties) AND"
      case None => cSQL""
    }
    val ledgerEndOffset = ledgerEndCache()._1
    SQL"""
        WITH relevant_offsets AS (
          SELECT
            party,
            max(ledger_offset) ledger_offset,
            #${queryStrategy.booleanOrAggregationFunction}(is_local) is_local
          FROM lapi_party_entries
          WHERE
            ledger_offset <= $ledgerEndOffset AND
            $partyFilter
            typ = 'accept'
          GROUP BY party
        )
        SELECT
          lapi_party_entries.party,
          lapi_party_entries.display_name,
          relevant_offsets.is_local
        FROM lapi_party_entries INNER JOIN relevant_offsets ON
          lapi_party_entries.party = relevant_offsets.party AND
          lapi_party_entries.ledger_offset = relevant_offsets.ledger_offset
       """.asVectorOf(partyDetailsParser)(connection)
  }

  override def parties(parties: Seq[Party])(connection: Connection): List[IndexerPartyDetails] =
    queryParties(Some(parties.view.map(_.toString).toSet), connection).toList

  override def knownParties(connection: Connection): List[IndexerPartyDetails] =
    queryParties(None, connection).toList

}
