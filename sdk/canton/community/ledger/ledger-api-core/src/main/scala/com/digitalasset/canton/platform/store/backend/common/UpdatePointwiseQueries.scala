// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.RowParser
import com.digitalasset.canton.data
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{Entry, RawTreeEvent}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning

import java.sql.Connection

class UpdatePointwiseQueries(
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
) {
  import EventStorageBackendTemplate.*

  /** Fetches a matching event sequential id range. */
  def fetchIdsFromTransactionMeta(
      lookupKey: LookupKey
  )(connection: Connection): Option[(Long, Long)] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.ledgerStringToStatement
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    // 1. Checking whether "event_offset <= ledgerEndOffset" is needed because during indexing
    // the events and transaction_meta tables are written to prior to the ledger end being updated.
    val ledgerEndOffsetO: Option[Offset] = ledgerEndCache().map(_.lastOffset)

    ledgerEndOffsetO.flatMap { ledgerEndOffset =>
      val lookupKeyClause: CompositeSql =
        lookupKey match {
          case LookupKey.UpdateId(updateId) =>
            cSQL"t.update_id = $updateId"
          case LookupKey.Offset(offset) =>
            cSQL"t.event_offset = $offset"
        }

      SQL"""
         SELECT
            t.event_sequential_id_first,
            t.event_sequential_id_last
         FROM
            lapi_transaction_meta t
         WHERE
            $lookupKeyClause
           AND
            t.event_offset <= $ledgerEndOffset
       """.as(EventSequentialIdFirstLast.singleOpt)(connection)
    }
  }

  // TODO(#23504) remove when transaction trees legacy endpoints are removed
  def fetchTreeTransactionEvents(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Option[Set[Party]],
  )(connection: Connection): Vector[Entry[RawTreeEvent]] =
    fetchEventsForTransactionPointWiseLookup(
      firstEventSequentialId = firstEventSequentialId,
      lastEventSequentialId = lastEventSequentialId,
      witnessesColumn = "tree_event_witnesses",
      tables = List(
        SelectTable(
          tableName = "lapi_events_create",
          selectColumns =
            s"$selectColumnsForTransactionTreeCreate, ${QueryStrategy.constBooleanSelect(false)} as exercise_consuming",
        ),
        SelectTable(
          tableName = "lapi_events_consuming_exercise",
          selectColumns =
            s"$selectColumnsForTransactionTreeExercise, ${QueryStrategy.constBooleanSelect(true)} as exercise_consuming",
        ),
        SelectTable(
          tableName = "lapi_events_non_consuming_exercise",
          selectColumns =
            s"$selectColumnsForTransactionTreeExercise, ${QueryStrategy.constBooleanSelect(false)} as exercise_consuming",
        ),
      ),
      requestingParties = requestingParties,
      filteringRowParser = rawTreeEventParser(_, stringInterning),
    )(connection)

  case class SelectTable(tableName: String, selectColumns: String)

  private def fetchEventsForTransactionPointWiseLookup[T](
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      witnessesColumn: String,
      tables: List[SelectTable],
      requestingParties: Option[Set[Party]],
      filteringRowParser: Option[Set[Int]] => RowParser[Entry[T]],
  )(connection: Connection): Vector[Entry[T]] = {
    val allInternedParties: Option[Set[Int]] = requestingParties.map(
      _.iterator
        .map(stringInterning.party.tryInternalize)
        .flatMap(_.iterator)
        .toSet
    )
    // Improvement idea: Add support for `fetchSizeHint` and `limit`.
    def selectFrom(tableName: String, selectColumns: String) = cSQL"""
        (
          SELECT
            #$selectColumns,
            event_witnesses,
            command_id
          FROM
          (
              SELECT
                #$selectColumns,
                #$witnessesColumn as event_witnesses,
                e.command_id
              FROM
                #$tableName e
              JOIN lapi_parameters p
              ON
                p.participant_pruned_up_to_inclusive IS NULL
                OR
                e.event_offset > p.participant_pruned_up_to_inclusive
              WHERE
                e.event_sequential_id >= $firstEventSequentialId
                AND
                e.event_sequential_id <= $lastEventSequentialId
              ORDER BY
                e.event_sequential_id
          ) x
        )
      """
    val unionQuery = tables
      .map(table =>
        selectFrom(
          tableName = table.tableName,
          selectColumns = table.selectColumns,
        )
      )
      .mkComposite("", " UNION ALL", "")
    val parsedRows: Vector[Entry[T]] = SQL"""
        $unionQuery
        ORDER BY event_sequential_id"""
      .asVectorOf(
        parser = filteringRowParser(allInternedParties)
      )(connection)
    parsedRows
  }

}

object UpdatePointwiseQueries {
  sealed trait LookupKey
  object LookupKey {
    final case class UpdateId(updateId: data.UpdateId) extends LookupKey
    final case class Offset(offset: data.Offset) extends LookupKey
  }
}
