// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, SimpleSql, SqlStringInterpolation}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.{Identifier => ApiIdentifier}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.dao.events.EventsTableQueries.{
  format,
  formatWhereCondition,
  previousOffsetWhereClauseValues
}

private[events] sealed trait EventsTableFlatEventsRangeQueries[Offset] {

  protected def singleWildcardParty(
      offset: Offset,
      party: Party,
      pageSize: Int,
      previousEventNodeIndex: Option[Int],
  ): SimpleSql[Row]

  protected def singlePartyWithTemplates(
      offset: Offset,
      party: Party,
      templateIds: Set[ApiIdentifier],
      pageSize: Int,
      previousEventNodeIndex: Option[Int],
  ): SimpleSql[Row]

  protected def onlyWildcardParties(
      offset: Offset,
      parties: Set[Party],
      pageSize: Int,
      previousEventNodeIndex: Option[Int],
  ): SimpleSql[Row]

  protected def sameTemplates(
      offset: Offset,
      parties: Set[Party],
      templateIds: Set[ApiIdentifier],
      pageSize: Int,
      previousEventNodeIndex: Option[Int],
  ): SimpleSql[Row]

  protected def mixedTemplates(
      offset: Offset,
      partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
      pageSize: Int,
      previousEventNodeIndex: Option[Int],
  ): SimpleSql[Row]

  protected def mixedTemplatesWithWildcardParties(
      offset: Offset,
      wildcardParties: Set[Party],
      partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
      pageSize: Int,
      previousEventNodeIndex: Option[Int],
  ): SimpleSql[Row]

  final def apply(
      offset: Offset,
      filter: FilterRelation,
      pageSize: Int,
      previousEventNodeIndex: Option[Int],
  ): SimpleSql[Row] = {
    require(filter.nonEmpty, "The request must be issued by at least one party")

    // Route the request to the correct underlying query
    if (filter.size == 1) {
      val (party, templateIds) = filter.toIterator.next
      if (templateIds.isEmpty) {
        // Single-party request, no specific template identifier
        singleWildcardParty(offset, party, pageSize, previousEventNodeIndex)
      } else {
        // Single-party request, restricted to a set of template identifiers
        singlePartyWithTemplates(offset, party, templateIds, pageSize, previousEventNodeIndex)
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
          previousEventNodeIndex = previousEventNodeIndex,
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
            previousEventNodeIndex = previousEventNodeIndex,
          )
        } else {
          // If there are different template identifier but there are no wildcard parties
          val partiesAndTemplateIds = Relation.flatten(filter).toSet
          val wildcardParties = filter.filter(_._2.isEmpty).keySet
          if (wildcardParties.isEmpty) {
            mixedTemplates(
              offset,
              partiesAndTemplateIds = partiesAndTemplateIds,
              pageSize = pageSize,
              previousEventNodeIndex = previousEventNodeIndex,
            )
          } else {
            // If there are wildcard parties and different template identifiers
            mixedTemplatesWithWildcardParties(
              offset,
              wildcardParties,
              partiesAndTemplateIds,
              pageSize,
              previousEventNodeIndex,
            )
          }
        }
      }
    }
  }

}

private[events] object EventsTableFlatEventsRangeQueries {

  final class GetTransactions(
      selectColumns: String,
      witnessesAggregation: String,
      flatEventsTable: String,
      groupByColumns: String,
      orderByColumns: String,
  ) extends EventsTableFlatEventsRangeQueries[(Offset, Offset)] {

    override protected def singleWildcardParty(
        between: (Offset, Offset),
        party: Party,
        pageSize: Int,
        previousEventNodeIndex: Option[Int],
    ): SimpleSql[Row] = {
      val (prevOffset, prevNodeIndex) =
        previousOffsetWhereClauseValues(between, previousEventNodeIndex)
      SQL"""select #$selectColumns, array[$party] as event_witnesses, case when submitter = $party then command_id else '' end as command_id from #$flatEventsTable where (event_offset > ${between._1} or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= ${between._2} and #$witnessesAggregation && array[$party]::varchar[] order by (#$orderByColumns) limit $pageSize"""
    }

    override protected def singlePartyWithTemplates(
        between: (Offset, Offset),
        party: Party,
        templateIds: Set[ApiIdentifier],
        pageSize: Int,
        previousEventNodeIndex: Option[Int],
    ): SimpleSql[Row] = {
      val (prevOffset, prevNodeIndex) =
        previousOffsetWhereClauseValues(between, previousEventNodeIndex)
      SQL"select #$selectColumns, array[$party] as event_witnesses, case when submitter = $party then command_id else '' end as command_id from #$flatEventsTable where (event_offset > ${between._1} or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= ${between._2} and #$witnessesAggregation && array[$party]::varchar[] and template_id in ($templateIds) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize"
    }

    protected def onlyWildcardParties(
        between: (Offset, Offset),
        parties: Set[Party],
        pageSize: Int,
        previousEventNodeIndex: Option[Int],
    ): SimpleSql[Row] = {
      val (prevOffset, prevNodeIndex) =
        previousOffsetWhereClauseValues(between, previousEventNodeIndex)
      val partiesStr = format(parties)
      SQL"select #$selectColumns, array(select unnest(#$witnessesAggregation) intersect select unnest(array[#$partiesStr])) as event_witnesses, case when submitter in (#$partiesStr) then command_id else '' end as command_id from #$flatEventsTable where (event_offset > ${between._1} or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= ${between._2} and #$witnessesAggregation && array[#$partiesStr]::varchar[] group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize"
    }

    protected def sameTemplates(
        between: (Offset, Offset),
        parties: Set[Party],
        templateIds: Set[ApiIdentifier],
        pageSize: Int,
        previousEventNodeIndex: Option[Int],
    ): SimpleSql[Row] = {
      val (prevOffset, prevNodeIndex) =
        previousOffsetWhereClauseValues(between, previousEventNodeIndex)
      val partiesStr = format(parties)
      SQL"select #$selectColumns, array(select unnest(#$witnessesAggregation) intersect select unnest(array[#$partiesStr])) as event_witnesses, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where (event_offset > ${between._1} or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= ${between._2} and #$witnessesAggregation && array[#$partiesStr]::varchar[] and template_id in ($templateIds) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize"
    }

    protected def mixedTemplates(
        between: (Offset, Offset),
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
        pageSize: Int,
        previousEventNodeIndex: Option[Int],
    ): SimpleSql[Row] = {
      val parties = partiesAndTemplateIds.map(_._1)
      val partiesStr = format(parties)
      val (prevOffset, prevNodeIndex) =
        previousOffsetWhereClauseValues(between, previousEventNodeIndex)
      val partiesAndTemplatesCondition =
        formatWhereCondition(witnessesAggregation, partiesAndTemplateIds)
      SQL"select #$selectColumns, array(select unnest(#$witnessesAggregation) intersect select unnest(array[#$partiesStr])) as event_witnesses, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where (event_offset > ${between._1} or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= ${between._2} and #$partiesAndTemplatesCondition group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize"
    }

    protected def mixedTemplatesWithWildcardParties(
        between: (Offset, Offset),
        wildcardParties: Set[Party],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
        pageSize: Int,
        previousEventNodeIndex: Option[Int],
    ): SimpleSql[Row] = {
      val parties = wildcardParties ++ partiesAndTemplateIds.map(_._1)
      val partiesStr = format(parties)
      val wildcardPartiesStr = format(wildcardParties)
      val (prevOffset, prevNodeIndex) =
        previousOffsetWhereClauseValues(between, previousEventNodeIndex)
      val partiesAndTemplatesCondition =
        formatWhereCondition(witnessesAggregation, partiesAndTemplateIds)
      SQL"select #$selectColumns, array(select unnest(#$witnessesAggregation) intersect select unnest(array[#$partiesStr])) as event_witnesses, case when submitter in (#$partiesStr) then command_id else '' end as command_id from #$flatEventsTable where (event_offset > ${between._1} or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= ${between._2} and (#$witnessesAggregation && array[#$wildcardPartiesStr]::varchar[] or #$partiesAndTemplatesCondition) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize"
    }

  }

  final class GetActiveContracts(
      selectColumns: String,
      witnessesAggregation: String,
      flatEventsTable: String,
      groupByColumns: String,
      orderByColumns: String,
  ) extends EventsTableFlatEventsRangeQueries[(Offset, Offset)] {

    override protected def singleWildcardParty(
        between: (Offset, Offset),
        party: Party,
        pageSize: Int,
        previousEventNodeIndex: Option[Int],
    ): SimpleSql[Row] = {
      val (prevOffset, prevNodeIndex) =
        previousOffsetWhereClauseValues(between, previousEventNodeIndex)
      SQL"select #$selectColumns, array[$party] as event_witnesses, case when submitter = $party then command_id else '' end as command_id from #$flatEventsTable where create_argument is not null and (event_offset > ${between._1} or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= ${between._2} and (create_consumed_at is null or create_consumed_at > ${between._2}) and  #$witnessesAggregation && array[$party]::varchar[] order by (#$orderByColumns) limit $pageSize"
    }

    override protected def singlePartyWithTemplates(
        between: (Offset, Offset),
        party: Party,
        templateIds: Set[ApiIdentifier],
        pageSize: Int,
        previousEventNodeIndex: Option[Int],
    ): SimpleSql[Row] = {
      val (prevOffset, prevNodeIndex) =
        previousOffsetWhereClauseValues(between, previousEventNodeIndex)
      SQL"select #$selectColumns, array[$party] as event_witnesses, case when submitter = $party then command_id else '' end as command_id from #$flatEventsTable where create_argument is not null and (event_offset > ${between._1} or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= ${between._2} and (create_consumed_at is null or create_consumed_at > ${between._2}) and  #$witnessesAggregation && array[$party]::varchar[] and template_id in ($templateIds) order by (#$orderByColumns) limit $pageSize"
    }

    def onlyWildcardParties(
        between: (Offset, Offset),
        parties: Set[Party],
        pageSize: Int,
        previousEventNodeIndex: Option[Int],
    ): SimpleSql[Row] = {
      val (prevOffset, prevNodeIndex) =
        previousOffsetWhereClauseValues(between, previousEventNodeIndex)
      val partiesStr = format(parties)
      SQL"select #$selectColumns, array(select unnest(#$witnessesAggregation) intersect select unnest(array[#$partiesStr])) as event_witnesses, case when submitter in (#$partiesStr) then command_id else '' end as command_id from #$flatEventsTable where create_argument is not null and (event_offset > ${between._1} or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= ${between._2} and (create_consumed_at is null or create_consumed_at > ${between._2}) and  #$witnessesAggregation && array[#$partiesStr]::varchar[] group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize"
    }

    def sameTemplates(
        between: (Offset, Offset),
        parties: Set[Party],
        templateIds: Set[ApiIdentifier],
        pageSize: Int,
        previousEventNodeIndex: Option[Int],
    ): SimpleSql[Row] = {
      val (prevOffset, prevNodeIndex) =
        previousOffsetWhereClauseValues(between, previousEventNodeIndex)
      val partiesStr = format(parties)
      SQL"select #$selectColumns, array(select unnest(#$witnessesAggregation) intersect select unnest(array[#$partiesStr])) as event_witnesses, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where create_argument is not null and (event_offset > ${between._1} or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= ${between._2} and (create_consumed_at is null or create_consumed_at > ${between._2}) and  #$witnessesAggregation && array[#$partiesStr]::varchar[] and template_id in ($templateIds) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize"
    }

    def mixedTemplates(
        between: (Offset, Offset),
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
        pageSize: Int,
        previousEventNodeIndex: Option[Int],
    ): SimpleSql[Row] = {
      val parties = partiesAndTemplateIds.map(_._1)
      val partiesStr = format(parties)
      val partiesAndTemplatesCondition =
        formatWhereCondition(witnessesAggregation, partiesAndTemplateIds)
      val (prevOffset, prevNodeIndex) =
        previousOffsetWhereClauseValues(between, previousEventNodeIndex)
      SQL"select #$selectColumns, array(select unnest(#$witnessesAggregation) intersect select unnest(array[#$partiesStr])) as event_witnesses, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where create_argument is not null and (event_offset > ${between._1} or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= ${between._2} and (create_consumed_at is null or create_consumed_at > ${between._2}) and #$partiesAndTemplatesCondition group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize"
    }

    def mixedTemplatesWithWildcardParties(
        between: (Offset, Offset),
        wildcardParties: Set[Party],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
        pageSize: Int,
        previousEventNodeIndex: Option[Int],
    ): SimpleSql[Row] = {
      val parties = wildcardParties ++ partiesAndTemplateIds.map(_._1)
      val partiesStr = format(parties)
      val (prevOffset, prevNodeIndex) =
        previousOffsetWhereClauseValues(between, previousEventNodeIndex)
      val wildcardPartiesStr = format(wildcardParties)
      val partiesAndTemplatesCondition =
        formatWhereCondition(witnessesAggregation, partiesAndTemplateIds)
      SQL"select #$selectColumns, array(select unnest(#$witnessesAggregation) intersect select unnest(array[#$partiesStr])) as event_witnesses, case when submitter in (#$partiesStr) then command_id else '' end as command_id from #$flatEventsTable where create_argument is not null and (event_offset > ${between._1} or (event_offset = $prevOffset and node_index > $prevNodeIndex)) and event_offset <= ${between._2} and (create_consumed_at is null or create_consumed_at > ${between._2}) and ( #$witnessesAggregation && array[#$wildcardPartiesStr]::varchar[] or #$partiesAndTemplatesCondition) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize"
    }
  }

}
