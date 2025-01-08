// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{bool, str}
import anorm.{RowParser, ~}
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.backend.PartyStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.cache.LedgerEndCache

import java.sql.Connection

class PartyStorageBackendTemplate(ledgerEndCache: LedgerEndCache) extends PartyStorageBackend {

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
        import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
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
