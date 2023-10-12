// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.long
import com.daml.lf.data.Ref
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlAsVectorOf.*
import com.digitalasset.canton.platform.store.dao.events.Raw
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{Identifier, Party}

import java.sql.Connection

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
  import EventStorageBackendTemplate.*

  def fetchEventIdsForStakeholder(target: EventIdSourceForStakeholders)(
      stakeholder: Party,
      templateIdO: Option[Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = target match {
    case EventIdSourceForStakeholders.Consuming =>
      fetchIdsOfConsumingEventsForStakeholder(
        stakeholder,
        templateIdO,
        startExclusive,
        endInclusive,
        limit,
      )(connection)
    case EventIdSourceForStakeholders.Create =>
      fetchIdsOfCreateEventsForStakeholder(
        stakeholder,
        templateIdO,
        startExclusive,
        endInclusive,
        limit,
      )(connection)
  }

  def fetchEventIdsForInformee(target: EventIdSourceForInformees)(
      informee: Party,
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = target match {
    case EventIdSourceForInformees.ConsumingStakeholder =>
      fetchIdsOfConsumingEventsForStakeholder(informee, None, startExclusive, endInclusive, limit)(
        connection
      )
    case EventIdSourceForInformees.ConsumingNonStakeholder =>
      TransactionStreamingQueries.fetchEventIds(
        tableName = "pe_consuming_id_filter_non_stakeholder_informee",
        witness = informee,
        templateIdO = None,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
        stringInterning = stringInterning,
      )(connection)
    case EventIdSourceForInformees.CreateStakeholder =>
      fetchIdsOfCreateEventsForStakeholder(informee, None, startExclusive, endInclusive, limit)(
        connection
      )
    case EventIdSourceForInformees.CreateNonStakeholder =>
      TransactionStreamingQueries.fetchEventIds(
        tableName = "pe_create_id_filter_non_stakeholder_informee",
        witness = informee,
        templateIdO = None,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
        stringInterning = stringInterning,
      )(connection)
    case EventIdSourceForInformees.NonConsumingInformee =>
      TransactionStreamingQueries.fetchEventIds(
        tableName = "pe_non_consuming_id_filter_informee",
        witness = informee,
        templateIdO = None,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
        stringInterning = stringInterning,
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

  def fetchIdsOfCreateEventsForStakeholder(
      stakeholder: Ref.Party,
      templateIdO: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = {
    TransactionStreamingQueries.fetchEventIds(
      tableName = "pe_create_id_filter_stakeholder",
      witness = stakeholder,
      templateIdO = templateIdO,
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      limit = limit,
      stringInterning = stringInterning,
    )(connection)
  }

  private def fetchIdsOfConsumingEventsForStakeholder(
      stakeholder: Ref.Party,
      templateIdO: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = {
    TransactionStreamingQueries.fetchEventIds(
      tableName = "pe_consuming_id_filter_stakeholder",
      witness = stakeholder,
      templateIdO = templateIdO,
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      limit = limit,
      stringInterning = stringInterning,
    )(connection)
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

object TransactionStreamingQueries {

  /** @param tableName   one of filter tables for create, consuming or non-consuming events
    * @param templateIdO NOTE: this parameter is not applicable for tree tx stream only oriented filters
    */
  def fetchEventIds(
      tableName: String,
      witness: Ref.Party,
      templateIdO: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
      stringInterning: StringInterning,
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

}
