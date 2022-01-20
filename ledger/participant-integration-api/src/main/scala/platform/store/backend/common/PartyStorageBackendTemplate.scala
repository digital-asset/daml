// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.{RowParser, ~}
import anorm.SqlParser.{bool, flatten, str}
import com.daml.ledger.api.domain.PartyDetails
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.platform.store.Conversions.{ledgerString, offset, party, timestampFromMicros}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.appendonlydao.JdbcLedgerDao.{acceptType, rejectType}
import com.daml.platform.store.backend.PartyStorageBackend
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.entries.PartyLedgerEntry

class PartyStorageBackendTemplate(queryStrategy: QueryStrategy, ledgerEndCache: LedgerEndCache)
    extends PartyStorageBackend {

  private val partyEntryParser: RowParser[(Offset, PartyLedgerEntry)] = {
    import com.daml.platform.store.Conversions.bigDecimalColumnToBoolean
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
              PartyDetails(party, displayNameOpt, isLocal),
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
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL"""select * from party_entries
      where ($startExclusive is null or ledger_offset>$startExclusive) and ledger_offset<=$endInclusive
      order by ledger_offset asc
      offset $queryOffset rows
      fetch next $pageSize rows only
      """
      .asVectorOf(partyEntryParser)(connection)
  }

  private val partyDetailsParser: RowParser[PartyDetails] = {
    import com.daml.platform.store.Conversions.bigDecimalColumnToBoolean
    str("party") ~
      str("display_name").? ~
      bool("is_local") map { case party ~ displayName ~ isLocal =>
        PartyDetails(
          party = Ref.Party.assertFromString(party),
          displayName = displayName,
          isLocal = isLocal,
        )
      }
  }

  private def queryParties(
      parties: Option[Set[String]],
      connection: Connection,
  ): Vector[PartyDetails] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    val partyFilter = parties match {
      case Some(requestedParties) => cSQL"party_entries.party in ($requestedParties) AND"
      case None => cSQL""
    }
    val ledgerEndOffset = ledgerEndCache()._1
    SQL"""
        WITH relevant_offsets AS (
          SELECT
            party,
            max(ledger_offset) ledger_offset,
            #${queryStrategy.booleanOrAggregationFunction}(is_local) is_local
          FROM party_entries
          WHERE
            ledger_offset <= $ledgerEndOffset AND
            $partyFilter
            typ = 'accept'
          GROUP BY party
        )
        SELECT
          party_entries.party,
          party_entries.display_name,
          relevant_offsets.is_local
        FROM party_entries INNER JOIN relevant_offsets ON
          party_entries.party = relevant_offsets.party AND
          party_entries.ledger_offset = relevant_offsets.ledger_offset
       """.asVectorOf(partyDetailsParser)(connection)
  }

  override def parties(parties: Seq[Ref.Party])(connection: Connection): List[PartyDetails] =
    queryParties(Some(parties.view.map(_.toString).toSet), connection).toList

  override def knownParties(connection: Connection): List[PartyDetails] =
    queryParties(None, connection).toList

}
