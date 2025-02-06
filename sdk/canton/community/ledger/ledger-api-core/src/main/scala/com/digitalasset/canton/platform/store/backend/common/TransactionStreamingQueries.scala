// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.long
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawFlatEvent,
  RawTreeEvent,
}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{Identifier, Party}
import com.digitalasset.daml.lf.data.Ref

import java.sql.Connection

sealed trait EventIdSource
object EventIdSource {
  object CreateStakeholder extends EventIdSource
  object CreateNonStakeholder extends EventIdSource
  object ConsumingStakeholder extends EventIdSource
  object ConsumingNonStakeholder extends EventIdSource
  object NonConsumingInformee extends EventIdSource
}
sealed trait EventPayloadSourceForUpdatesAcsDelta
object EventPayloadSourceForUpdatesAcsDelta {
  object Create extends EventPayloadSourceForUpdatesAcsDelta
  object Consuming extends EventPayloadSourceForUpdatesAcsDelta
}
sealed trait EventPayloadSourceForUpdatesLedgerEffects
object EventPayloadSourceForUpdatesLedgerEffects {
  object Create extends EventPayloadSourceForUpdatesLedgerEffects
  object Consuming extends EventPayloadSourceForUpdatesLedgerEffects
  object NonConsuming extends EventPayloadSourceForUpdatesLedgerEffects
}

class TransactionStreamingQueries(
    queryStrategy: QueryStrategy,
    stringInterning: StringInterning,
) {
  import EventStorageBackendTemplate.*

  def fetchEventIds(target: EventIdSource)(
      stakeholderO: Option[Party],
      templateIdO: Option[Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = target match {
    case EventIdSource.ConsumingStakeholder =>
      fetchIdsOfConsumingEventsForStakeholder(
        stakeholder = stakeholderO,
        templateIdO = templateIdO,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
      )(
        connection
      )
    case EventIdSource.ConsumingNonStakeholder =>
      TransactionStreamingQueries.fetchEventIds(
        tableName = "lapi_pe_consuming_id_filter_non_stakeholder_informee",
        witnessO = stakeholderO,
        templateIdO = templateIdO,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
        stringInterning = stringInterning,
      )(connection)
    case EventIdSource.CreateStakeholder =>
      fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = stakeholderO,
        templateIdO = templateIdO,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
      )(
        connection
      )
    case EventIdSource.CreateNonStakeholder =>
      TransactionStreamingQueries.fetchEventIds(
        tableName = "lapi_pe_create_id_filter_non_stakeholder_informee",
        witnessO = stakeholderO,
        templateIdO = templateIdO,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
        stringInterning = stringInterning,
      )(connection)
    case EventIdSource.NonConsumingInformee =>
      TransactionStreamingQueries.fetchEventIds(
        tableName = "lapi_pe_non_consuming_id_filter_informee",
        witnessO = stakeholderO,
        templateIdO = templateIdO,
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        limit = limit,
        stringInterning = stringInterning,
      )(connection)
  }

  def fetchEventPayloadsAcsDelta(target: EventPayloadSourceForUpdatesAcsDelta)(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Ref.Party]],
  )(connection: Connection): Vector[Entry[RawFlatEvent]] =
    target match {
      case EventPayloadSourceForUpdatesAcsDelta.Consuming =>
        fetchAcsDeltaEvents(
          tableName = "lapi_events_consuming_exercise",
          selectColumns = selectColumnsForFlatTransactionsExercise,
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
      case EventPayloadSourceForUpdatesAcsDelta.Create =>
        fetchAcsDeltaEvents(
          tableName = "lapi_events_create",
          selectColumns = selectColumnsForFlatTransactionsCreate,
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
    }

  def fetchEventPayloadsLedgerEffects(target: EventPayloadSourceForUpdatesLedgerEffects)(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Ref.Party]],
  )(connection: Connection): Vector[Entry[RawTreeEvent]] =
    target match {
      case EventPayloadSourceForUpdatesLedgerEffects.Consuming =>
        fetchLedgerEffectsEvents(
          tableName = "lapi_events_consuming_exercise",
          selectColumns =
            s"$selectColumnsForTransactionTreeExercise, ${QueryStrategy.constBooleanSelect(true)} as exercise_consuming",
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
      case EventPayloadSourceForUpdatesLedgerEffects.Create =>
        fetchLedgerEffectsEvents(
          tableName = "lapi_events_create",
          selectColumns =
            s"$selectColumnsForTransactionTreeCreate, ${QueryStrategy.constBooleanSelect(false)} as exercise_consuming",
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
      case EventPayloadSourceForUpdatesLedgerEffects.NonConsuming =>
        fetchLedgerEffectsEvents(
          tableName = "lapi_events_non_consuming_exercise",
          selectColumns =
            s"$selectColumnsForTransactionTreeExercise, ${QueryStrategy.constBooleanSelect(false)} as exercise_consuming",
          eventSequentialIds = eventSequentialIds,
          allFilterParties = allFilterParties,
        )(connection)
    }

  def fetchIdsOfCreateEventsForStakeholder(
      stakeholderO: Option[Ref.Party],
      templateIdO: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] =
    TransactionStreamingQueries.fetchEventIds(
      tableName = "lapi_pe_create_id_filter_stakeholder",
      witnessO = stakeholderO,
      templateIdO = templateIdO,
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      limit = limit,
      stringInterning = stringInterning,
    )(connection)

  private def fetchIdsOfConsumingEventsForStakeholder(
      stakeholder: Option[Ref.Party],
      templateIdO: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] =
    TransactionStreamingQueries.fetchEventIds(
      tableName = "lapi_pe_consuming_id_filter_stakeholder",
      witnessO = stakeholder,
      templateIdO = templateIdO,
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      limit = limit,
      stringInterning = stringInterning,
    )(connection)

  private def fetchAcsDeltaEvents(
      tableName: String,
      selectColumns: String,
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Ref.Party]],
  )(connection: Connection): Vector[Entry[RawFlatEvent]] = {
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
      .asVectorOf(rawAcsDeltaEventParser(internedAllParties, stringInterning))(connection)
  }

  private def fetchLedgerEffectsEvents(
      tableName: String,
      selectColumns: String,
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Ref.Party]],
  )(connection: Connection): Vector[Entry[RawTreeEvent]] = {
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

  // TODO(i22416): Rename the arguments of this function, as witnessO and templateIdO are inadequate for party topology events.
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
