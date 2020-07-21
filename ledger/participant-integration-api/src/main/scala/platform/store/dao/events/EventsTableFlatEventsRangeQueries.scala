// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, SimpleSql, SqlStringInterpolation}
import com.daml.lf.data.Ref.{Identifier => ApiIdentifier}
import com.daml.platform.store.Conversions._

private[events] sealed trait EventsTableFlatEventsRangeQueries[Offset] {

  protected def singleWildcardParty(
      offset: Offset,
      party: Party,
      pageSize: Int,
  ): SimpleSql[Row]

  protected def singlePartyWithTemplates(
      offset: Offset,
      party: Party,
      templateIds: Set[ApiIdentifier],
      pageSize: Int,
  ): SimpleSql[Row]

  protected def onlyWildcardParties(
      offset: Offset,
      parties: Set[Party],
      pageSize: Int,
  ): SimpleSql[Row]

  protected def sameTemplates(
      offset: Offset,
      parties: Set[Party],
      templateIds: Set[ApiIdentifier],
      pageSize: Int,
  ): SimpleSql[Row]

  protected def mixedTemplates(
      offset: Offset,
      partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
      pageSize: Int,
  ): SimpleSql[Row]

  protected def mixedTemplatesWithWildcardParties(
      offset: Offset,
      wildcardParties: Set[Party],
      partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
      pageSize: Int,
  ): SimpleSql[Row]

  final def apply(
      offset: Offset,
      filter: FilterRelation,
      pageSize: Int,
  ): SimpleSql[Row] = {
    require(filter.nonEmpty, "The request must be issued by at least one party")

    // Route the request to the correct underlying query
    if (filter.size == 1) {
      val (party, templateIds) = filter.toIterator.next
      if (templateIds.isEmpty) {
        // Single-party request, no specific template identifier
        singleWildcardParty(offset, party, pageSize)
      } else {
        // Single-party request, restricted to a set of template identifiers
        singlePartyWithTemplates(offset, party, templateIds, pageSize)
      }
    } else {
      // Multi-party requests
      // If no party requests specific template identifiers
      val parties = filter.keySet
      if (filter.forall(_._2.isEmpty))
        onlyWildcardParties(
          offset = offset,
          parties = parties,
          pageSize = pageSize,
        )
      else {
        // If all parties request the same template identifier
        val templateIds = filter.valuesIterator.flatten.toSet
        if (filter.valuesIterator.forall(_ == templateIds)) {
          sameTemplates(
            offset,
            parties = parties,
            templateIds = templateIds,
            pageSize = pageSize,
          ).map(r => r)
        } else {
          // If there are different template identifier but there are no wildcard parties
          val partiesAndTemplateIds = Relation.flatten(filter).toSet
          val wildcardParties = filter.filter(_._2.isEmpty).keySet
          if (wildcardParties.isEmpty) {
            mixedTemplates(
              offset,
              partiesAndTemplateIds = partiesAndTemplateIds,
              pageSize = pageSize,
            )
          } else {
            // If there are wildcard parties and different template identifiers
            mixedTemplatesWithWildcardParties(
              offset,
              wildcardParties,
              partiesAndTemplateIds,
              pageSize,
            )
          }
        }
      }
    }
  }
}

private[events] object EventsTableFlatEventsRangeQueries {

  import com.daml.ledger.participant.state.v1.Offset

  final class GetTransactions(
      selectColumns: String,
      groupByColumns: String,
  ) extends EventsTableFlatEventsRangeQueries[EventsRange[Long]] {

    override protected def singleWildcardParty(
        range: EventsRange[Long],
        party: Party,
        pageSize: Int,
    ): SimpleSql[Row] = {
      SQL"""select #$selectColumns, array[$party] as event_witnesses, case when submitter = $party then command_id else '' end as command_id from participant_events where event_sequential_id > ${range.startExclusive} and event_sequential_id <= ${range.endInclusive} and da_do_varchar_arrays_intersect(flat_event_witnesses::varchar[], array[$party]) order by event_sequential_id limit $pageSize"""
    }

    override protected def singlePartyWithTemplates(
        range: EventsRange[Long],
        party: Party,
        templateIds: Set[ApiIdentifier],
        pageSize: Int,
    ): SimpleSql[Row] = {
      SQL"select #$selectColumns, array[$party] as event_witnesses, case when submitter = $party then command_id else '' end as command_id from participant_events where event_sequential_id > ${range.startExclusive} and event_sequential_id <= ${range.endInclusive} and da_do_varchar_arrays_intersect(flat_event_witnesses::varchar[], array[$party]) and template_id in ($templateIds) group by (#$groupByColumns) order by event_sequential_id limit $pageSize"
    }

    protected def onlyWildcardParties(
        range: EventsRange[Long],
        parties: Set[Party],
        pageSize: Int,
    ): SimpleSql[Row] = {
      SQL"select #$selectColumns, da_varchar_arrays_intersection(flat_event_witnesses::varchar[], $parties) as event_witnesses, case when submitter in ($parties) then command_id else '' end as command_id from participant_events where event_sequential_id > ${range.startExclusive} and event_sequential_id <= ${range.endInclusive} and da_do_varchar_arrays_intersect(flat_event_witnesses::varchar[], $parties) group by (#$groupByColumns) order by event_sequential_id limit $pageSize"
    }

    protected def sameTemplates(
        range: EventsRange[Long],
        parties: Set[Party],
        templateIds: Set[ApiIdentifier],
        pageSize: Int,
    ): SimpleSql[Row] = {
      SQL"select #$selectColumns, da_varchar_arrays_intersection(flat_event_witnesses::varchar[], $parties) as event_witnesses, case when submitter in ($parties) then command_id else '' end as command_id from participant_events where event_sequential_id > ${range.startExclusive} and event_sequential_id <= ${range.endInclusive} and da_do_varchar_arrays_intersect(flat_event_witnesses::varchar[], $parties) and template_id in ($templateIds) group by (#$groupByColumns) order by event_sequential_id limit $pageSize"
    }

    protected def mixedTemplates(
        range: EventsRange[Long],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
        pageSize: Int,
    ): SimpleSql[Row] = {
      val parties = partiesAndTemplateIds.map(_._1)
      val partiesAndTemplatesCondition =
        formatPartiesAndTemplatesWhereClause("flat_event_witnesses", partiesAndTemplateIds)
      SQL"select #$selectColumns, da_varchar_arrays_intersection(flat_event_witnesses::varchar[], $parties) as event_witnesses, case when submitter in ($parties) then command_id else '' end as command_id from participant_events where event_sequential_id > ${range.startExclusive} and event_sequential_id <= ${range.endInclusive} and #$partiesAndTemplatesCondition group by (#$groupByColumns) order by event_sequential_id limit $pageSize"
    }

    protected def mixedTemplatesWithWildcardParties(
        range: EventsRange[Long],
        wildcardParties: Set[Party],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
        pageSize: Int,
    ): SimpleSql[Row] = {
      val parties = wildcardParties ++ partiesAndTemplateIds.map(_._1)
      val partiesAndTemplatesCondition =
        formatPartiesAndTemplatesWhereClause("flat_event_witnesses", partiesAndTemplateIds)
      SQL"select #$selectColumns, da_varchar_arrays_intersection(flat_event_witnesses::varchar[], $parties) as event_witnesses, case when submitter in ($parties) then command_id else '' end as command_id from participant_events where event_sequential_id > ${range.startExclusive} and event_sequential_id <= ${range.endInclusive} and (da_do_varchar_arrays_intersect(flat_event_witnesses::varchar[], $wildcardParties) or #$partiesAndTemplatesCondition) group by (#$groupByColumns) order by event_sequential_id limit $pageSize"
    }
  }

  final class GetActiveContracts(
      selectColumns: String,
      groupByColumns: String,
  ) extends EventsTableFlatEventsRangeQueries[EventsRange[(Offset, Long)]] {

    override protected def singleWildcardParty(
        range: EventsRange[(Offset, Long)],
        party: Party,
        pageSize: Int,
    ): SimpleSql[Row] = {
      SQL"select #$selectColumns, array[$party] as event_witnesses, case when submitter = $party then command_id else '' end as command_id from participant_events where create_argument is not null and event_sequential_id > ${range.startExclusive._2: Long} and event_sequential_id <= ${range.endInclusive._2: Long} and (create_consumed_at is null or create_consumed_at > ${range.endInclusive._1: Offset}) and da_do_varchar_arrays_intersect(flat_event_witnesses::varchar[], array[$party]) order by event_sequential_id limit $pageSize"
    }

    override protected def singlePartyWithTemplates(
        range: EventsRange[(Offset, Long)],
        party: Party,
        templateIds: Set[ApiIdentifier],
        pageSize: Int,
    ): SimpleSql[Row] = {
      SQL"select #$selectColumns, array[$party] as event_witnesses, case when submitter = $party then command_id else '' end as command_id from participant_events where create_argument is not null and event_sequential_id > ${range.startExclusive._2: Long} and event_sequential_id <= ${range.endInclusive._2: Long} and (create_consumed_at is null or create_consumed_at > ${range.endInclusive._1: Offset}) and da_do_varchar_arrays_intersect(flat_event_witnesses::varchar[], array[$party]) and template_id in ($templateIds) order by event_sequential_id limit $pageSize"
    }

    def onlyWildcardParties(
        range: EventsRange[(Offset, Long)],
        parties: Set[Party],
        pageSize: Int,
    ): SimpleSql[Row] = {
      SQL"select #$selectColumns, da_varchar_arrays_intersection(flat_event_witnesses::varchar[], $parties) as event_witnesses, case when submitter in ($parties) then command_id else '' end as command_id from participant_events where create_argument is not null and event_sequential_id > ${range.startExclusive._2: Long} and event_sequential_id <= ${range.endInclusive._2: Long} and (create_consumed_at is null or create_consumed_at > ${range.endInclusive._1: Offset}) and da_do_varchar_arrays_intersect(flat_event_witnesses::varchar[], $parties) group by (#$groupByColumns) order by event_sequential_id limit $pageSize"
    }

    def sameTemplates(
        range: EventsRange[(Offset, Long)],
        parties: Set[Party],
        templateIds: Set[ApiIdentifier],
        pageSize: Int,
    ): SimpleSql[Row] = {
      SQL"select #$selectColumns, da_varchar_arrays_intersection(flat_event_witnesses::varchar[], $parties) as event_witnesses, case when submitter in ($parties) then command_id else '' end as command_id from participant_events where create_argument is not null and event_sequential_id > ${range.startExclusive._2: Long} and event_sequential_id <= ${range.endInclusive._2: Long} and (create_consumed_at is null or create_consumed_at > ${range.endInclusive._1: Offset}) and da_do_varchar_arrays_intersect(flat_event_witnesses::varchar[], $parties) and template_id in ($templateIds) group by (#$groupByColumns) order by event_sequential_id limit $pageSize"
    }

    def mixedTemplates(
        range: EventsRange[(Offset, Long)],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
        pageSize: Int,
    ): SimpleSql[Row] = {
      val parties = partiesAndTemplateIds.map(_._1)
      val partiesAndTemplatesCondition =
        formatPartiesAndTemplatesWhereClause("flat_event_witnesses", partiesAndTemplateIds)
      SQL"select #$selectColumns, da_varchar_arrays_intersection(flat_event_witnesses::varchar[], $parties) as event_witnesses, case when submitter in ($parties) then command_id else '' end as command_id from participant_events where create_argument is not null and event_sequential_id > ${range.startExclusive._2: Long} and event_sequential_id <= ${range.endInclusive._2: Long} and (create_consumed_at is null or create_consumed_at > ${range.endInclusive._1: Offset}) and #$partiesAndTemplatesCondition group by (#$groupByColumns) order by event_sequential_id limit $pageSize"
    }

    def mixedTemplatesWithWildcardParties(
        range: EventsRange[(Offset, Long)],
        wildcardParties: Set[Party],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
        pageSize: Int,
    ): SimpleSql[Row] = {
      val parties = wildcardParties ++ partiesAndTemplateIds.map(_._1)
      val partiesAndTemplatesCondition =
        formatPartiesAndTemplatesWhereClause("flat_event_witnesses", partiesAndTemplateIds)
      SQL"select #$selectColumns, da_varchar_arrays_intersection(flat_event_witnesses::varchar[], $parties) as event_witnesses, case when submitter in ($parties) then command_id else '' end as command_id from participant_events where create_argument is not null and event_sequential_id > ${range.startExclusive._2: Long} and event_sequential_id <= ${range.endInclusive._2: Long} and (create_consumed_at is null or create_consumed_at > ${range.endInclusive._1: Offset}) and (da_do_varchar_arrays_intersect(flat_event_witnesses::varchar[], $wildcardParties) or #$partiesAndTemplatesCondition) group by (#$groupByColumns) order by event_sequential_id limit $pageSize"
    }
  }

  // TODO(LEO) String Injection: convert to user-defined SQL function
  private def formatPartiesAndTemplatesWhereClause(
      witnessesAggregationColumn: String,
      partiesAndTemplateIds: Set[(Party, Identifier)]
  ): String =
    partiesAndTemplateIds.view
      .map {
        case (p, i) =>
          s"(da_do_varchar_arrays_intersect('$witnessesAggregationColumn', array['$p']) and template_id = '$i')"
      }
      .mkString("(", " or ", ")")
}
