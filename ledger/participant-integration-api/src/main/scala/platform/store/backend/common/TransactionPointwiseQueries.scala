// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.RowParser
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.platform.Party
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.dao.events.Raw
import com.daml.platform.store.interning.StringInterning
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation

class TransactionPointwiseQueries(
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
) {
  import EventStorageBackendTemplate._

  /** Fetches a matching event sequential id range unless it's within the pruning offset.
    */
  def fetchIdsFromTransactionMeta(
      transactionId: Ref.TransactionId
  )(connection: Connection): Option[(Long, Long)] = {
    import com.daml.platform.store.backend.Conversions.ledgerStringToStatement
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    // NOTE: Is checking whether "event_offset <= ledgerEndOffset" needed?
    //              It is because when new updates are indexed their events are written first and only after that the ledger end gets updated.
    //              Re: The row from transaction_meta table itself might be beyond the ledger end. So in order to prevent accessing it we need this guard.
    // NOTE: Checking against participant_pruned_up_to_inclusive to avoid fetching data that
    //              within the pruning offset. Such data shall not be retrieved from transaction based endpoints.
    //              Rather, such data shall be retrieved only by the ACS endpoint.
    val ledgerEndOffset: Offset = ledgerEndCache()._1
    // TODO pbatko: ? rename from, to -> first, last
    SQL"""
         SELECT
            t.event_sequential_id_from,
            t.event_sequential_id_to
         FROM
            participant_transaction_meta t
         JOIN parameters p
         ON
            p.participant_pruned_up_to_inclusive IS NULL
            OR
            t.event_offset > p.participant_pruned_up_to_inclusive
         WHERE
            t.transaction_id = $transactionId
            AND
            t.event_offset <= $ledgerEndOffset
       """.as(EventSequentailIdFromTo.singleOpt)(connection)
  }

  def fetchFlatTransaction(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Set[Party],
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.FlatEvent]] = {
    fetchEventsForTransactionPointWiseLookup(
      firstEventSequentialId = firstEventSequentialId,
      lastEventSequentialId = lastEventSequentialId,
      witnessesColumn = "flat_event_witnesses",
      tables = List(
        SelectTable(
          tableName = "participant_events_create",
          selectColumns = selectColumnsForFlatTransactionsCreate,
        ),
        SelectTable(
          tableName = "participant_events_consuming_exercise",
          selectColumns = selectColumnsForFlatTransactionsExercise,
        ),
      ),
      requestingParties = requestingParties,
      filteringRowParser = rawFlatEventParser(_, stringInterning),
    )(connection)
  }

  def fetchTreeTransaction(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Set[Party],
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.TreeEvent]] = {
    fetchEventsForTransactionPointWiseLookup(
      firstEventSequentialId = firstEventSequentialId,
      lastEventSequentialId = lastEventSequentialId,
      witnessesColumn = "tree_event_witnesses",
      tables = List(
        SelectTable(
          tableName = "participant_events_create",
          selectColumns =
            s"$selectColumnsForTransactionTreeCreate, ${queryStrategy.constBooleanSelect(false)} as exercise_consuming",
        ),
        SelectTable(
          tableName = "participant_events_consuming_exercise",
          selectColumns =
            s"$selectColumnsForTransactionTreeExercise, ${queryStrategy.constBooleanSelect(true)} as exercise_consuming",
        ),
        SelectTable(
          tableName = "participant_events_non_consuming_exercise",
          selectColumns =
            s"$selectColumnsForTransactionTreeExercise, ${queryStrategy.constBooleanSelect(false)} as exercise_consuming",
        ),
      ),
      requestingParties = requestingParties,
      filteringRowParser = rawTreeEventParser(_, stringInterning),
    )(connection)
  }

  case class SelectTable(tableName: String, selectColumns: String)

  private def fetchEventsForTransactionPointWiseLookup[T](
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      witnessesColumn: String,
      tables: List[SelectTable],
      requestingParties: Set[Party],
      filteringRowParser: Set[Int] => RowParser[EventStorageBackend.Entry[T]],
  )(connection: Connection): Vector[EventStorageBackend.Entry[T]] = {
    val allInternedParties: Set[Int] = requestingParties.iterator
      .map(stringInterning.party.tryInternalize)
      .flatMap(_.iterator)
      .toSet
    // TODO pbatko: Consider implementing support for `fetchSizeHint` and `limit`.
    // TODO pbatko: Note that we are checking against fetching data from the pruned offset
    //              even though the same check has been done when fetching event seq ids from transaction_meta table.
    //              Both checks are needed as fetching from transaction_meta and fetching events here
    //              happens in two different transactions which can be interleaved by a pruning transaction.
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
              JOIN parameters p
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
    val parsedRows: Vector[EventStorageBackend.Entry[T]] = SQL"""
        $unionQuery
        ORDER BY event_sequential_id"""
      .asVectorOf(
        parser = filteringRowParser(allInternedParties)
      )(connection)
    parsedRows
  }

}
