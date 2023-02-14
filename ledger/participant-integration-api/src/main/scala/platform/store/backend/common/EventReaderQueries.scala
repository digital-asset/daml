// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import anorm.RowParser
import com.daml.lf.data.Ref.Party
import com.daml.lf.value.Value.ContractId
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._
import com.daml.platform.store.dao.events.Raw
import com.daml.platform.store.dao.events.Raw.FlatEvent
import com.daml.platform.store.interning.StringInterning

import java.sql.Connection
import scala.annotation.tailrec

class EventReaderQueries(
    queryStrategy: QueryStrategy,
    stringInterning: StringInterning,
) {
  import EventStorageBackendTemplate._

  type EventSequentialId = Long

  case class SelectTable(tableName: String, selectColumns: String)

  def fetchContractIdEvents(
      contractId: ContractId,
      requestingParties: Set[Party],
      endEventSequentialId: EventSequentialId,
  )(
      connection: Connection
  ): Vector[EventStorageBackend.Entry[Raw.FlatEvent]] = {

    val witnessesColumn = "flat_event_witnesses"

    val tables = List(
      SelectTable(
        tableName = "participant_events_create",
        selectColumns =
          s"$selectColumnsForFlatTransactionsCreate, ${queryStrategy.constBooleanSelect(false)} as exercise_consuming",
      ),
      SelectTable(
        tableName = "participant_events_consuming_exercise",
        selectColumns =
          s"$selectColumnsForFlatTransactionsExercise, ${queryStrategy.constBooleanSelect(true)} as exercise_consuming",
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
              command_id
            FROM #$tableName
            WHERE contract_id = ${contractId.coid}
            AND event_sequential_id <=$endEventSequentialId
            ORDER BY event_sequential_id
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

    query.asVectorOf(eventParser(requestingParties))(connection)

  }

  private def selectLatestKeyCreateEvent(
      keyHash: String,
      intRequestingParties: Set[Int],
      extRequestingParties: Set[String],
      lastExclusiveSeqId: EventSequentialId,
  )(conn: Connection): Option[EventStorageBackend.Entry[FlatEvent.Created]] = {

    @tailrec def go(
        endExclusiveSeqId: EventSequentialId
    ): Option[EventStorageBackend.Entry[FlatEvent.Created]] = {
      val query =
        SQL"""
        WITH max_event AS (
            SELECT max(c.event_sequential_id) AS sequential_id
            FROM participant_events_create c
            WHERE c.create_key_hash = $keyHash
            AND c.event_sequential_id < $endExclusiveSeqId)
        SELECT  #$selectColumnsForFlatTransactionsCreate,
                #${queryStrategy.constBooleanSelect(false)} exercise_consuming,
                flat_event_witnesses event_witnesses,
                command_id
        FROM max_event
        JOIN participant_events_create c on c.event_sequential_id = max_event.sequential_id
      """
      query.as(createdFlatEventParser(intRequestingParties, stringInterning).singleOpt)(
        conn
      ) match {
        case Some(c) if c.event.stakeholders.exists(extRequestingParties) => Some(c)
        case Some(c) => go(c.eventSequentialId)
        case None => None
      }
    }
    go(lastExclusiveSeqId)
  }

  private def selectArchivedEvent(contractId: String, intRequestingParties: Set[Int])(
      conn: Connection
  ): Option[EventStorageBackend.Entry[FlatEvent.Archived]] = {
    val query =
      SQL"""
          SELECT  #$selectColumnsForFlatTransactionsExercise,
                  #${queryStrategy.constBooleanSelect(true)} exercise_consuming,
                  flat_event_witnesses event_witnesses,
                  command_id
          FROM participant_events_consuming_exercise
          WHERE contract_id = $contractId
        """
    query.as(archivedFlatEventParser(intRequestingParties, stringInterning).singleOpt)(conn)
  }

  def fetchNextKeyEvents(
      keyHash: String,
      requestingParties: Set[Party],
      endExclusiveSeqId: EventSequentialId,
  )(
      conn: Connection
  ): (Option[Raw.FlatEvent.Created], Option[Raw.FlatEvent.Archived], Option[EventSequentialId]) = {

    val intRequestingParties =
      requestingParties.iterator.map(stringInterning.party.tryInternalize).flatMap(_.iterator).toSet

    val createEvent = selectLatestKeyCreateEvent(
      keyHash,
      intRequestingParties,
      requestingParties.map(identity),
      endExclusiveSeqId,
    )(conn)
    val archivedEvent = createEvent.flatMap(c =>
      selectArchivedEvent(c.event.partial.contractId, intRequestingParties)(conn)
    )

    (createEvent.map(_.event), archivedEvent.map(_.event), createEvent.map(_.eventSequentialId))
  }

  private def eventParser(
      requestingParties: Set[Party]
  ): RowParser[EventStorageBackend.Entry[Raw.FlatEvent]] =
    rawFlatEventParser(
      requestingParties.iterator
        .map(stringInterning.party.tryInternalize)
        .flatMap(_.iterator)
        .toSet,
      stringInterning,
    )

}
