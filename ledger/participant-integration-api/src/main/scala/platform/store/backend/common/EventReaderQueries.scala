// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import anorm.{RowParser, SqlParser}
import com.daml.lf.data.Ref.Party
import com.daml.lf.value.Value.ContractId
import com.daml.platform.store.EventSequentialId
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._
import com.daml.platform.store.dao.events.Raw
import com.daml.platform.store.interning.StringInterning

import java.sql.Connection

class EventReaderQueries(
    queryStrategy: QueryStrategy,
    stringInterning: StringInterning,
) {
  import EventStorageBackendTemplate._

  type EventSequentialId = Long

  case class SelectTable(tableName: String, selectColumns: String)

  def fetchContractIdEvents(contractId: ContractId, requestingParties: Set[Party])(
      connection: Connection
  ): Vector[EventStorageBackend.Entry[Raw.TreeEvent]] = {

    val witnessesColumn = "tree_event_witnesses"

    val tables = List(
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
    )

    def selectFrom(tableName: String, selectColumns: String) =
      cSQL"""
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
              e.contract_id = ${contractId.coid}
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

    val query = SQL"""$unionQuery ORDER BY event_sequential_id"""

    query.asVectorOf(treeEventParser(requestingParties))(connection)

  }

  private def fetchSeqIdOfPrecedingCreate(keyHash: String, startExclusive: EventSequentialId)(
      connection: Connection
  ): Option[EventSequentialId] = {
    // As we are working in the preceding period include the start-exclusive
    val parser = SqlParser.get[EventSequentialId](1).?
    val query = SQL"""
      SELECT max(event_sequential_id)
      FROM participant_events_create
      WHERE create_key_hash = $keyHash
      AND event_sequential_id <= $startExclusive
    """
    query.as(parser.single)(connection)
  }

  def fetchContractKeyEvents(
      keyHash: String,
      requestingParties: Set[Party],
      maxEvents: Int,
      startExclusive: EventSequentialId,
      endInclusive: EventSequentialId,
  )(
      connection: Connection
  ): Vector[EventStorageBackend.Entry[Raw.TreeEvent]] = {

    val precedingCreateSeq = fetchSeqIdOfPrecedingCreate(keyHash, startExclusive)(connection)
      .getOrElse(EventSequentialId.beforeBegin)

    val query = SQL"""
        WITH key_contract AS (
            SELECT e.contract_id cid
            FROM participant_events_create e
            JOIN parameters p ON p.participant_pruned_up_to_inclusive IS NULL OR event_offset > p.participant_pruned_up_to_inclusive
            WHERE create_key_hash = $keyHash
            AND e.event_sequential_id >= $precedingCreateSeq
            AND e.event_sequential_id <= $endInclusive
            ORDER BY e.event_sequential_id
            FETCH FIRST #$maxEvents ROWS ONLY
        ), pre_filtered AS (
            SELECT  #$selectColumnsForTransactionTreeCreate,
                    #${queryStrategy.constBooleanSelect(false)} exercise_consuming,
                    tree_event_witnesses event_witnesses,
                    command_id
            FROM key_contract
            JOIN participant_events_create e on e.contract_id = key_contract.cid
            UNION ALL
            SELECT  #$selectColumnsForTransactionTreeExercise,
                    #${queryStrategy.constBooleanSelect(true)} exercise_consuming,
                    tree_event_witnesses event_witnesses,
                    command_id
            FROM key_contract
            JOIN participant_events_consuming_exercise e on e.contract_id = key_contract.cid
        )
        SELECT *
        FROM pre_filtered
        WHERE event_sequential_id > $startExclusive
        AND event_sequential_id <= $endInclusive
        ORDER BY event_sequential_id
        FETCH FIRST #$maxEvents ROWS ONLY
       """

    query.asVectorOf(treeEventParser(requestingParties))(connection)

  }

  private def treeEventParser(
      requestingParties: Set[Party]
  ): RowParser[EventStorageBackend.Entry[Raw.TreeEvent]] =
    rawTreeEventParser(
      requestingParties.iterator
        .map(stringInterning.party.tryInternalize)
        .flatMap(_.iterator)
        .toSet,
      stringInterning,
    )

}
