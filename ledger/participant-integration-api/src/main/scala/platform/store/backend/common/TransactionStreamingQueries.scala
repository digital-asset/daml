// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import com.daml.lf.data.Ref
import com.daml.platform.{Identifier, Party}
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._
import com.daml.platform.store.dao.events.Raw
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.interning.StringInterning
import anorm.SqlParser.long

sealed trait EventIdSourceForStakeholders
object EventIdSourceForStakeholders {
  object Create extends EventIdSourceForStakeholders
  object Consuming extends EventIdSourceForStakeholders
}
sealed trait EventIdSourceForInformees
object EventIdSourceForInformees {
  object CreateStakeholder extends EventIdSourceForInformees
  object CreateNonStakeholder extends EventIdSourceForInformees
  object ConsumingStakeholder extends EventIdSourceForInformees
  object ConsumingNonStakeholder extends EventIdSourceForInformees
  object NonConsumingInformee extends EventIdSourceForInformees
}
sealed trait EventPayloadSourceForFlatTx
object EventPayloadSourceForFlatTx {
  object Create extends EventPayloadSourceForFlatTx
  object Consuming extends EventPayloadSourceForFlatTx
}
sealed trait EventPayloadSourceForTreeTx
object EventPayloadSourceForTreeTx {
  object Create extends EventPayloadSourceForTreeTx
  object Consuming extends EventPayloadSourceForTreeTx
  object NonConsuming extends EventPayloadSourceForTreeTx
}

class TransactionStreamingQueries(
    queryStrategy: QueryStrategy,
    stringInterning: StringInterning,
) {
  import EventStorageBackendTemplate._

  def fetchEventIdsForStakeholder(target: EventIdSourceForStakeholders)(
      stakeholder: Party,
      templateIdO: Option[Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = target match {
    case EventIdSourceForStakeholders.Consuming =>
      fetchIdsOfConsumingEventsForStakeholders(
        stakeholder,
        templateIdO,
        startExclusive,
        endInclusive,
        limit,
      )(connection)
    case EventIdSourceForStakeholders.Create =>
      fetchIdsOfCreateEventsForStakeholders(
        stakeholder,
        templateIdO,
        startExclusive,
        endInclusive,
        limit,
      )(connection)
  }

  def fetchEventIdsForInformees(target: EventIdSourceForInformees)(
      informee: Party,
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = target match {
    case EventIdSourceForInformees.ConsumingStakeholder =>
      fetchIdsOfConsumingEventsForStakeholders(informee, None, startExclusive, endInclusive, limit)(
        connection
      )
    case EventIdSourceForInformees.ConsumingNonStakeholder =>
      fetchEventIds(
        tableName = "pe_consuming_exercise_filter_nonstakeholder_informees",
        witness = informee,
        templateIdO = None,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
      )(connection)
    case EventIdSourceForInformees.CreateStakeholder =>
      fetchIdsOfCreateEventsForStakeholders(informee, None, startExclusive, endInclusive, limit)(
        connection
      )
    case EventIdSourceForInformees.CreateNonStakeholder =>
      fetchEventIds(
        tableName = "pe_create_filter_nonstakeholder_informees",
        witness = informee,
        templateIdO = None,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
      )(connection)
    case EventIdSourceForInformees.NonConsumingInformee =>
      fetchEventIds(
        tableName = "pe_non_consuming_exercise_filter_informees",
        witness = informee,
        templateIdO = None,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
      )(connection)
  }

  def fetchEventPayloadsFlat(target: EventPayloadSourceForFlatTx)(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Ref.Party],
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.FlatEvent]] = {
    target match {
      case EventPayloadSourceForFlatTx.Consuming =>
        fetchFlatEvents(
          tableName = "participant_events_consuming_exercise",
          selectColumns = selectColumnsForFlatTransactionsExercise,
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
      case EventPayloadSourceForFlatTx.Create =>
        fetchFlatEvents(
          tableName = "participant_events_create",
          selectColumns = selectColumnsForFlatTransactionsCreate,
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
    }
  }

  def fetchEventPayloadsTree(target: EventPayloadSourceForTreeTx)(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Ref.Party],
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.TreeEvent]] = {
    target match {
      case EventPayloadSourceForTreeTx.Consuming =>
        fetchTreeEvents(
          tableName = "participant_events_consuming_exercise",
          selectColumns =
            s"$selectColumnsForTransactionTreeExercise, ${queryStrategy.constBooleanSelect(true)} as exercise_consuming",
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
      case EventPayloadSourceForTreeTx.Create =>
        fetchTreeEvents(
          tableName = "participant_events_create",
          selectColumns =
            s"$selectColumnsForTransactionTreeCreate, ${queryStrategy.constBooleanSelect(false)} as exercise_consuming",
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
      case EventPayloadSourceForTreeTx.NonConsuming =>
        fetchTreeEvents(
          tableName = "participant_events_non_consuming_exercise",
          selectColumns =
            s"$selectColumnsForTransactionTreeExercise, ${queryStrategy.constBooleanSelect(false)} as exercise_consuming",
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
    }
  }

  def fetchIdsOfCreateEventsForStakeholders(
      partyFilter: Ref.Party,
      templateIdFilter: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = {
    fetchEventIds(
      tableName = "participant_events_create_filter",
      witness = partyFilter,
      templateIdO = templateIdFilter,
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      limit = limit,
    )(connection)
  }

  private def fetchIdsOfConsumingEventsForStakeholders(
      stakeholder: Ref.Party,
      templateIdO: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = {
    fetchEventIds(
      tableName = "pe_consuming_exercise_filter_stakeholders",
      witness = stakeholder,
      templateIdO = templateIdO,
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      limit = limit,
    )(connection)
  }

  /** @param tableName one of filter tables for create, consuming or non-consuming events
    * @param templateIdO NOTE: this parameter is not applicable for tree tx stream only oriented filters
    */
  private def fetchEventIds(
      tableName: String,
      witness: Ref.Party,
      templateIdO: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = {
    (
      stringInterning.party.tryInternalize(witness),
      templateIdO.map(stringInterning.templateId.tryInternalize),
    ) match {
      case (None, _) => Vector.empty // partyFilter never seen
      case (_, Some(None)) => Vector.empty // templateIdFilter never seen
      case (Some(internedPartyFilter), internedTemplateIdFilterNested: Option[Option[Int]]) =>
        val (templateIdFilterClause, templateIdOrderingClause) =
          internedTemplateIdFilterNested.flatten // flatten works for both None, Some(Some(x)) case, Some(None) excluded before
          match {
            case Some(internedTemplateId) =>
              (
                cSQL"AND filters.template_id = $internedTemplateId",
                cSQL"filters.template_id,",
              )
            case None => (cSQL"", cSQL"")
          }
        SQL"""
         SELECT filters.event_sequential_id
         FROM
           #$tableName filters
         WHERE
           filters.party_id = $internedPartyFilter
           $templateIdFilterClause
           AND $startExclusive < event_sequential_id
           AND event_sequential_id <= $endInclusive
         ORDER BY
           filters.party_id,
           $templateIdOrderingClause
           filters.event_sequential_id -- deliver in index order
         ${QueryStrategy.limitClause(Some(limit))}
       """
          .asVectorOf(long("event_sequential_id"))(connection)
    }
  }

  private def fetchFlatEvents(
      tableName: String,
      selectColumns: String,
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Ref.Party],
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.FlatEvent]] = {
    val internedAllParties: Set[Int] = allFilterParties.iterator
      .map(stringInterning.party.tryInternalize)
      .flatMap(_.iterator)
      .toSet
    SQL"""
        SELECT
          #$selectColumns,
          flat_event_witnesses as event_witnesses,
          command_id
        FROM
          #$tableName
        WHERE
          event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
        ORDER BY
          event_sequential_id
      """
      .withFetchSize(Some(eventSequentialIds.size))
      .asVectorOf(rawFlatEventParser(internedAllParties, stringInterning))(connection)
  }

  private def fetchTreeEvents(
      tableName: String,
      selectColumns: String,
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Ref.Party],
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.TreeEvent]] = {
    val internedAllParties: Set[Int] = allFilterParties.iterator
      .map(stringInterning.party.tryInternalize)
      .flatMap(_.iterator)
      .toSet
    SQL"""
        SELECT
          #$selectColumns,
          tree_event_witnesses as event_witnesses,
          command_id
        FROM
          #$tableName
        WHERE
          event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
        ORDER BY
          event_sequential_id
      """
      .withFetchSize(Some(eventSequentialIds.size))
      .asVectorOf(rawTreeEventParser(internedAllParties, stringInterning))(connection)
  }

}
