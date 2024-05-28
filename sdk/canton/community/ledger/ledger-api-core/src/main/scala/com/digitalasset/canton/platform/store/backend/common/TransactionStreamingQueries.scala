// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
      stakeholderO: Option[Party],
      templateIdO: Option[Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = target match {
    case EventIdSourceForStakeholders.Consuming =>
      fetchIdsOfConsumingEventsForStakeholder(
        stakeholderO,
        templateIdO,
        startExclusive,
        endInclusive,
        limit,
      )(connection)
    case EventIdSourceForStakeholders.Create =>
      fetchIdsOfCreateEventsForStakeholder(
        stakeholderO,
        templateIdO,
        startExclusive,
        endInclusive,
        limit,
      )(connection)
  }

  def fetchEventIdsForInformee(target: EventIdSourceForInformees)(
      informeeO: Option[Party],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = target match {
    case EventIdSourceForInformees.ConsumingStakeholder =>
      fetchIdsOfConsumingEventsForStakeholder(informeeO, None, startExclusive, endInclusive, limit)(
        connection
      )
    case EventIdSourceForInformees.ConsumingNonStakeholder =>
      TransactionStreamingQueries.fetchEventIds(
        tableName = "lapi_pe_consuming_id_filter_non_stakeholder_informee",
        witnessO = informeeO,
        templateIdO = None,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
        stringInterning = stringInterning,
      )(connection)
    case EventIdSourceForInformees.CreateStakeholder =>
      fetchIdsOfCreateEventsForStakeholder(informeeO, None, startExclusive, endInclusive, limit)(
        connection
      )
    case EventIdSourceForInformees.CreateNonStakeholder =>
      TransactionStreamingQueries.fetchEventIds(
        tableName = "lapi_pe_create_id_filter_non_stakeholder_informee",
        witnessO = informeeO,
        templateIdO = None,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
        stringInterning = stringInterning,
      )(connection)
    case EventIdSourceForInformees.NonConsumingInformee =>
      TransactionStreamingQueries.fetchEventIds(
        tableName = "lapi_pe_non_consuming_id_filter_informee",
        witnessO = informeeO,
        templateIdO = None,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
        stringInterning = stringInterning,
      )(connection)
  }

  def fetchEventPayloadsFlat(target: EventPayloadSourceForFlatTx)(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Ref.Party]],
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.FlatEvent]] = {
    target match {
      case EventPayloadSourceForFlatTx.Consuming =>
        fetchFlatEvents(
          tableName = "lapi_events_consuming_exercise",
          selectColumns = selectColumnsForFlatTransactionsExercise,
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
      case EventPayloadSourceForFlatTx.Create =>
        fetchFlatEvents(
          tableName = "lapi_events_create",
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
          tableName = "lapi_events_consuming_exercise",
          selectColumns =
            s"$selectColumnsForTransactionTreeExercise, ${queryStrategy.constBooleanSelect(true)} as exercise_consuming",
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
      case EventPayloadSourceForTreeTx.Create =>
        fetchTreeEvents(
          tableName = "lapi_events_create",
          selectColumns =
            s"$selectColumnsForTransactionTreeCreate, ${queryStrategy.constBooleanSelect(false)} as exercise_consuming",
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
      case EventPayloadSourceForTreeTx.NonConsuming =>
        fetchTreeEvents(
          tableName = "lapi_events_non_consuming_exercise",
          selectColumns =
            s"$selectColumnsForTransactionTreeExercise, ${queryStrategy.constBooleanSelect(false)} as exercise_consuming",
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
    }
  }

  def fetchIdsOfCreateEventsForStakeholder(
      stakeholderO: Option[Ref.Party],
      templateIdO: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = {
    TransactionStreamingQueries.fetchEventIds(
      tableName = "lapi_pe_create_id_filter_stakeholder",
      witnessO = stakeholderO,
      templateIdO = templateIdO,
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      limit = limit,
      stringInterning = stringInterning,
    )(connection)
  }

  private def fetchIdsOfConsumingEventsForStakeholder(
      stakeholder: Option[Ref.Party],
      templateIdO: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = {
    TransactionStreamingQueries.fetchEventIds(
      tableName = "lapi_pe_consuming_id_filter_stakeholder",
      witnessO = stakeholder,
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
      allFilterParties: Option[Set[Ref.Party]],
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.FlatEvent]] = {
    val internedAllParties: Option[Set[Int]] = allFilterParties
      .map(
        _.iterator
          .map(stringInterning.party.tryInternalize)
          .flatMap(_.iterator)
          .toSet
      )
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
    * @param witnessO    the party for which to fetch the event ids, if None the event ids for all the parties should be fetched
    * @param templateIdO NOTE: this parameter is not applicable for tree tx stream only oriented filters
    */
  def fetchEventIds(
      tableName: String,
      witnessO: Option[Ref.Party],
      templateIdO: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
      stringInterning: StringInterning,
  )(connection: Connection): Vector[Long] = {
    val partyIdFilterO = witnessO match {
      case Some(witness) =>
        stringInterning.party
          .tryInternalize(witness) match {
          case Some(internedPartyFilter) =>
            // use ordering by party_id even though we are restricting the query to a single party_id
            // to ensure that the correct db index is used
            Some((cSQL"AND filters.party_id = $internedPartyFilter", cSQL"filters.party_id,"))
          case None => None // partyFilter never seen
        }
      case None =>
        // do not filter by party, fetch event for all parties
        Some((cSQL"", cSQL""))
    }

    val templateIdFilterO = templateIdO.map(stringInterning.templateId.tryInternalize) match {
      case Some(None) => None // templateIdFilter never seen
      case internedTemplateIdFilterNested =>
        internedTemplateIdFilterNested.flatten // flatten works for both None, Some(Some(x)) case, Some(None) excluded before
        match {
          case Some(internedTemplateId) =>
            // use ordering by template_id even though we are restricting the query to a single template_id
            // to ensure that the correct db index is used
            Some((cSQL"AND filters.template_id = $internedTemplateId", cSQL"filters.template_id,"))
          case None => Some((cSQL"", cSQL""))
        }
    }

    (partyIdFilterO, templateIdFilterO) match {
      case (
            Some((partyIdFilterClause, partyIdOrderingClause)),
            Some((templateIdFilterClause, templateIdOrderingClause)),
          ) =>
        SQL"""
         SELECT filters.event_sequential_id
         FROM
           #$tableName filters
         WHERE
           $startExclusive < event_sequential_id
           AND event_sequential_id <= $endInclusive
           $partyIdFilterClause
           $templateIdFilterClause
         ORDER BY
           $partyIdOrderingClause
           $templateIdOrderingClause
           filters.event_sequential_id -- deliver in index order
         ${QueryStrategy.limitClause(Some(limit))}
       """
          .asVectorOf(long("event_sequential_id"))(connection)
      case _ => Vector.empty
    }
  }

}
