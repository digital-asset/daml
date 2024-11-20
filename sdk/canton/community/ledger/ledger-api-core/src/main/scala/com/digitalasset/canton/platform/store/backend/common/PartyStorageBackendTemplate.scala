// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{bool, flatten, str}
import anorm.{RowParser, ~}
import com.digitalasset.canton.data.AbsoluteOffset
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.backend.Conversions.{
  absoluteOffset,
  ledgerString,
  party,
  timestampFromMicros,
}
import com.digitalasset.canton.platform.store.backend.PartyStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.dao.JdbcLedgerDao.{acceptType, rejectType}
import com.digitalasset.canton.platform.store.entries.PartyLedgerEntry

import java.sql.Connection

class PartyStorageBackendTemplate(ledgerEndCache: LedgerEndCache) extends PartyStorageBackend {

  private val partyEntryParser: RowParser[(AbsoluteOffset, PartyLedgerEntry)] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.bigDecimalColumnToBoolean
    (absoluteOffset("ledger_offset") ~
      timestampFromMicros("recorded_at") ~
      ledgerString("submission_id").? ~
      party("party").? ~
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
              `acceptType`,
              None,
              Some(isLocal),
            ) =>
          offset ->
            PartyLedgerEntry.AllocationAccepted(
              submissionIdOpt,
              recordTime,
              IndexerPartyDetails(party, isLocal),
            )
        case (
              offset,
              recordTime,
              Some(submissionId),
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
      startInclusive: AbsoluteOffset,
      endInclusive: AbsoluteOffset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(AbsoluteOffset, PartyLedgerEntry)] =
    SQL"""select * from lapi_party_entries
      where ${QueryStrategy.offsetIsBetweenInclusive(
        nonNullableColumn = "ledger_offset",
        startInclusive = startInclusive,
        endInclusive = endInclusive,
      )}
      order by ledger_offset asc
      offset $queryOffset rows
      fetch next $pageSize rows only
      """
      .asVectorOf(partyEntryParser)(connection)

  private val partyDetailsParser: RowParser[IndexerPartyDetails] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.bigDecimalColumnToBoolean
    str("party") ~
      bool("is_local") map { case party ~ isLocal =>
        IndexerPartyDetails(
          party = Party.assertFromString(party),
          isLocal = isLocal,
        )
      }
  }

  private def queryParties(
      partyFilter: ComposableQuery.CompositeSql,
      limitClause: ComposableQuery.CompositeSql,
      connection: Connection,
  ): Vector[IndexerPartyDetails] =
    ledgerEndCache() match {
      case None => Vector.empty
      case Some(ledgerEnd) =>
        import com.digitalasset.canton.platform.store.backend.Conversions.AbsoluteOffsetToStatement
        SQL"""
        SELECT
          party,
          #${QueryStrategy.booleanOrAggregationFunction}(is_local) is_local
        FROM lapi_party_entries
        WHERE
          ledger_offset <= ${ledgerEnd.lastOffset} AND
          $partyFilter
          typ = 'accept'
        GROUP BY party
        ORDER BY party
        $limitClause
       """.asVectorOf(partyDetailsParser)(connection)
    }

  override def parties(parties: Seq[Party])(connection: Connection): List[IndexerPartyDetails] = {
    val requestedParties = parties.view.map(_.toString).toSet
    val partyFilter = cSQL"lapi_party_entries.party in ($requestedParties) AND"
    queryParties(partyFilter, cSQL"", connection).toList
  }

  override def knownParties(fromExcl: Option[Party], maxResults: Int)(
      connection: Connection
  ): List[IndexerPartyDetails] = {
    val partyFilter = fromExcl match {
      case Some(id: String) => cSQL"lapi_party_entries.party > $id AND"
      case None => cSQL""
    }
    queryParties(
      partyFilter,
      cSQL"fetch next $maxResults rows only",
      connection,
    ).toList
  }

}
