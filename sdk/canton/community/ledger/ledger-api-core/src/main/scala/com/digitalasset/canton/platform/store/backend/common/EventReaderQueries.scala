// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.RowParser
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawAcsDeltaEvent,
  RawAcsDeltaEventLegacy,
  RawArchivedEventLegacy,
  RawCreatedEventLegacy,
}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.value.Value.ContractId

import java.sql.Connection
import scala.annotation.tailrec

class EventReaderQueries(stringInterning: StringInterning) {
  import EventStorageBackendTemplate.*

  type EventSequentialId = Long

  case class SelectTable(tableName: String, selectColumns: String)

  def fetchContractIdEvents(
      contractId: ContractId,
      requestingParties: Option[Set[Party]],
      endEventSequentialId: EventSequentialId,
  )(
      connection: Connection
  ): Vector[Entry[RawAcsDeltaEvent]] =
    ??? // TODO(#28002): Implement

  def fetchContractIdEventsLegacy(
      contractId: ContractId,
      requestingParties: Option[Set[Party]],
      endEventSequentialId: EventSequentialId,
  )(
      connection: Connection
  ): Vector[Entry[RawAcsDeltaEventLegacy]] = {

    val witnessesColumn = "flat_event_witnesses"

    val tables = List(
      SelectTable(
        tableName = "lapi_events_create",
        selectColumns =
          s"$selectColumnsForFlatTransactionsCreate, ${QueryStrategy.constBooleanSelect(false)} as exercise_consuming",
      ),
      SelectTable(
        tableName = "lapi_events_consuming_exercise",
        selectColumns =
          s"$selectColumnsForFlatTransactionsExercise, ${QueryStrategy.constBooleanSelect(true)} as exercise_consuming",
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
            WHERE contract_id = ${contractId.toBytes.toByteArray}
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
      requestingParties: Set[Party],
      extRequestingParties: Set[String],
      lastExclusiveSeqId: EventSequentialId,
      maxIterations: Int,
  )(
      conn: Connection
  ): (Option[Entry[RawCreatedEventLegacy]], Option[EventSequentialId]) = {

    @tailrec def go(
        endExclusiveSeqId: EventSequentialId,
        iterations: Int,
    ): (Option[Entry[RawCreatedEventLegacy]], Option[EventSequentialId]) = {
      val query =
        SQL"""
        WITH max_event AS (
            SELECT max(c.event_sequential_id) AS sequential_id
            FROM lapi_events_create c
            WHERE c.create_key_hash = $keyHash
            AND c.event_sequential_id < $endExclusiveSeqId)
        SELECT  #$selectColumnsForFlatTransactionsCreate,
                #${QueryStrategy.constBooleanSelect(false)} exercise_consuming,
                flat_event_witnesses event_witnesses,
                command_id
        FROM max_event
        JOIN lapi_events_create c on c.event_sequential_id = max_event.sequential_id
      """
      query.as(createdEventParser(Some(requestingParties), stringInterning).singleOpt)(
        conn
      ) match {
        case Some(c) if c.event.witnessParties.exists(extRequestingParties) =>
          (Some(c), Some(c.eventSequentialId))
        case Some(c) if iterations >= maxIterations => (None, Some(c.eventSequentialId))
        case Some(c) => go(c.eventSequentialId, iterations + 1)
        case None => (None, None)
      }
    }
    go(lastExclusiveSeqId, 1)
  }

  private def selectArchivedEvent(contractId: Array[Byte], requestingParties: Set[Party])(
      conn: Connection
  ): Option[Entry[RawArchivedEventLegacy]] = {
    val query =
      SQL"""
          SELECT  #$selectColumnsForFlatTransactionsExercise,
                  #${QueryStrategy.constBooleanSelect(true)} exercise_consuming,
                  flat_event_witnesses event_witnesses,
                  command_id
          FROM lapi_events_consuming_exercise
          WHERE contract_id = $contractId
        """
    query.as(archivedEventParser(Some(requestingParties), stringInterning).singleOpt)(conn)
  }

  def fetchNextKeyEvents(
      keyHash: String,
      requestingParties: Set[Party],
      endExclusiveSeqId: EventSequentialId,
      maxIterations: Int,
  )(
      conn: Connection
  ): (Option[RawCreatedEventLegacy], Option[RawArchivedEventLegacy], Option[EventSequentialId]) = {

    val (createEvent, continuationToken) = selectLatestKeyCreateEvent(
      keyHash,
      requestingParties,
      requestingParties.map(identity),
      endExclusiveSeqId,
      maxIterations,
    )(conn)
    val archivedEvent =
      createEvent.flatMap(c =>
        selectArchivedEvent(c.event.contractId.toBytes.toByteArray, requestingParties)(conn)
      )

    (createEvent.map(_.event), archivedEvent.map(_.event), continuationToken)
  }

  private def eventParser(
      requestingParties: Option[Set[Party]]
  ): RowParser[Entry[RawAcsDeltaEventLegacy]] =
    rawAcsDeltaEventParser(
      requestingParties,
      stringInterning,
    )

}
