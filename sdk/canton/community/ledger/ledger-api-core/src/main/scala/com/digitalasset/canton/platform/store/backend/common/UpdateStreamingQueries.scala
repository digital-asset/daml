// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.long
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.backend.PersistentEventType
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.{
  IdFilterInput,
  IdFilterPaginationInput,
  PaginationInput,
  PaginationLastOnlyInput,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.NameTypeConRef

import java.sql.Connection

sealed trait EventIdSource
object EventIdSource {
  object ActivateStakeholder extends EventIdSource
  object ActivateWitnesses extends EventIdSource
  object DeactivateStakeholder extends EventIdSource
  object DeactivateWitnesses extends EventIdSource
  object VariousWitnesses extends EventIdSource
}
sealed trait EventPayloadSourceForUpdatesAcsDelta
object EventPayloadSourceForUpdatesAcsDelta {
  object Activate extends EventPayloadSourceForUpdatesAcsDelta
  object Deactivate extends EventPayloadSourceForUpdatesAcsDelta
}
sealed trait EventPayloadSourceForUpdatesLedgerEffects
object EventPayloadSourceForUpdatesLedgerEffects {
  object Activate extends EventPayloadSourceForUpdatesLedgerEffects
  object Deactivate extends EventPayloadSourceForUpdatesLedgerEffects
  object VariousWitnessed extends EventPayloadSourceForUpdatesLedgerEffects
}

class UpdateStreamingQueries(
    stringInterning: StringInterning,
    queryStrategy: QueryStrategy,
) {

  def fetchEventIds(target: EventIdSource)(
      witnessO: Option[Party],
      templateIdO: Option[NameTypeConRef],
      eventTypes: Set[PersistentEventType],
  )(connection: Connection): IdFilterPaginationInput => Vector[Long] = {
    def idFilter(tableName: String): Option[CompositeSql] = Option.when(eventTypes.nonEmpty)(
      cSQL"""
          EXISTS (
            SELECT 1
            FROM #$tableName data_table
            WHERE
              filters.event_sequential_id = data_table.event_sequential_id
              AND data_table.event_type ${queryStrategy.anyOfSmallInts(eventTypes.map(_.asInt))}
          )"""
    )
    target match {
      case EventIdSource.ActivateStakeholder =>
        UpdateStreamingQueries.fetchEventIds(
          tableName = "lapi_filter_activate_stakeholder",
          witnessO = witnessO,
          templateIdO = templateIdO,
          idFilter = idFilter("lapi_events_activate_contract"),
          stringInterning = stringInterning,
          hasFirstPerSequentialId = true,
        )(connection)
      case EventIdSource.ActivateWitnesses =>
        UpdateStreamingQueries.fetchEventIds(
          tableName = "lapi_filter_activate_witness",
          witnessO = witnessO,
          templateIdO = templateIdO,
          idFilter = idFilter("lapi_events_activate_contract"),
          stringInterning = stringInterning,
          hasFirstPerSequentialId = true,
        )(connection)
      case EventIdSource.DeactivateStakeholder =>
        UpdateStreamingQueries.fetchEventIds(
          tableName = "lapi_filter_deactivate_stakeholder",
          witnessO = witnessO,
          templateIdO = templateIdO,
          idFilter = idFilter("lapi_events_deactivate_contract"),
          stringInterning = stringInterning,
          hasFirstPerSequentialId = true,
        )(connection)
      case EventIdSource.DeactivateWitnesses =>
        UpdateStreamingQueries.fetchEventIds(
          tableName = "lapi_filter_deactivate_witness",
          witnessO = witnessO,
          templateIdO = templateIdO,
          idFilter = idFilter("lapi_events_deactivate_contract"),
          stringInterning = stringInterning,
          hasFirstPerSequentialId = true,
        )(connection)
      case EventIdSource.VariousWitnesses =>
        UpdateStreamingQueries.fetchEventIds(
          tableName = "lapi_filter_various_witness",
          witnessO = witnessO,
          templateIdO = templateIdO,
          idFilter = idFilter("lapi_events_various_witnessed"),
          stringInterning = stringInterning,
          hasFirstPerSequentialId = true,
        )(connection)
    }
  }

  def fetchActiveIds(
      stakeholderO: Option[Ref.Party],
      templateIdO: Option[NameTypeConRef],
      activeAtEventSeqId: Long,
  )(connection: Connection): IdFilterPaginationInput => Vector[Long] =
    UpdateStreamingQueries.fetchEventIds(
      tableName = "lapi_filter_activate_stakeholder",
      witnessO = stakeholderO,
      templateIdO = templateIdO,
      idFilter = Some(
        cSQL"""
          NOT EXISTS (
            SELECT 1
            FROM lapi_events_deactivate_contract deactivate_evs
            WHERE
              filters.event_sequential_id = deactivate_evs.deactivated_event_sequential_id
              AND deactivate_evs.event_sequential_id <= $activeAtEventSeqId
          )"""
      ),
      stringInterning = stringInterning,
      hasFirstPerSequentialId = true,
    )(connection)
}

object UpdateStreamingQueries {

  // TODO(i22416): Rename the arguments of this function, as witnessO and templateIdO are inadequate for party topology events.
  /** @param tableName
    *   one of the filter tables for create, consuming or non-consuming events
    * @param witnessO
    *   the party for which to fetch the event ids, if None the event ids for all the parties should
    *   be fetched
    * @param templateIdO
    *   NOTE: this parameter is not applicable for tree tx stream only oriented filters
    * @param idFilter
    *   Inside of the composable SQL the event_sequential_id-s of the candidates need to be referred
    *   as ''filters.event_sequential_id''. EXISTS and NOT EXISTS expressions suggested to trigger
    *   pointwise iteration on target indexes.
    * @param stringInterning
    *   the string interning instance to use for internalizing the party and template id
    * @param hasFirstPerSequentialId
    *   true if the table has the first_per_sequential_id column, false otherwise. If true and
    *   witnessO is None, only a single row per event_sequential_id will be fetched and thus only
    *   unique event ids will be returned
    */
  def fetchEventIds(
      tableName: String,
      witnessO: Option[Ref.Party],
      templateIdO: Option[NameTypeConRef],
      idFilter: Option[CompositeSql],
      stringInterning: StringInterning,
      hasFirstPerSequentialId: Boolean,
  )(connection: Connection): IdFilterPaginationInput => Vector[Long] = {
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

    // if we do not filter by party and the table has first_per_sequential_id column, we only fetch a single row per event_sequential_id
    val firstPerSequentialIdClause = witnessO match {
      case None if hasFirstPerSequentialId =>
        cSQL"AND filters.first_per_sequential_id = true"
      case _ => cSQL""
    }

    (partyIdFilterO, templateIdFilterO) match {
      case (
            Some((partyIdFilterClause, partyIdOrderingClause)),
            Some((templateIdFilterClause, templateIdOrderingClause)),
          ) =>
        def filterTableSelect(
            startExclusive: Long,
            endInclusive: Long,
            limit: Option[Int],
            idFilter: Option[CompositeSql],
        ): CompositeSql =
          cSQL"""
            SELECT filters.event_sequential_id event_sequential_id
            FROM
              #$tableName filters
            WHERE
              $startExclusive < filters.event_sequential_id
              AND filters.event_sequential_id <= $endInclusive
              $partyIdFilterClause
              $templateIdFilterClause
              $firstPerSequentialIdClause
              ${idFilter.map(f => cSQL"AND $f").getOrElse(cSQL"")}
            ORDER BY
              $partyIdOrderingClause
              $templateIdOrderingClause
              filters.event_sequential_id -- deliver in index order
            ${limit.map(l => cSQL"LIMIT $l").getOrElse(cSQL"")}"""
        idPaginationInput =>
          val sql = idPaginationInput match {
            case PaginationInput(startExclusive, endInclusive, limit) =>
              filterTableSelect(
                startExclusive = startExclusive,
                endInclusive = endInclusive,
                limit = Some(limit),
                idFilter =
                  None, // disable regardless - this is the case where we reuse the query for a no-ID-filter population case
              )

            case IdFilterInput(_, _) if idFilter.isEmpty =>
              throw new IllegalStateException(
                "Using non-id-filter compliant query for ID filtration. In this case the ID filter needs to be defined"
              )

            case IdFilterInput(startExclusive, endInclusive) =>
              filterTableSelect(
                startExclusive = startExclusive,
                endInclusive = endInclusive,
                limit = None,
                idFilter = idFilter,
              )

            case PaginationLastOnlyInput(_, _, _) if idFilter.isEmpty =>
              throw new IllegalStateException(
                "Using non-id-filter compliant query for ID filtration. In this case the ID filter needs to be defined"
              )

            case PaginationLastOnlyInput(startExclusive, endInclusive, limit) =>
              val filterTableSQL = filterTableSelect(
                startExclusive = startExclusive,
                endInclusive = endInclusive,
                limit = Some(limit),
                idFilter =
                  None, // disable regardless - this is the case where we reuse the query for a ID-filter population: the paginated query
              )
              cSQL"""
                WITH unfiltered_ids AS (
                $filterTableSQL
                )
                SELECT unfiltered_ids.event_sequential_id event_sequential_id
                FROM unfiltered_ids
                ORDER BY event_sequential_id DESC
                LIMIT 1"""
          }
          SQL"$sql".asVectorOf(long("event_sequential_id"))(connection)

      case _ => _ => Vector.empty
    }
  }
}
