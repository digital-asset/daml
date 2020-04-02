// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao.events

import anorm.{Row, SimpleSql, SqlStringInterpolation}
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.platform.store.Conversions._

private[events] sealed trait EventsTableFlatEventsRangeQueries[Offset] {

  protected def onlyWildcardParties(
      offset: Offset,
      parties: Set[Party],
      pageSize: Int,
      rowOffset: Long,
  ): SimpleSql[Row]

  protected def sameTemplates(
      offset: Offset,
      parties: Set[Party],
      templateIds: Set[Identifier],
      pageSize: Int,
      rowOffset: Long,
  ): SimpleSql[Row]

  protected def mixedTemplates(
      offset: Offset,
      partiesAndTemplateIds: Set[(Party, Identifier)],
      pageSize: Int,
      rowOffset: Long,
  ): SimpleSql[Row]

  protected def mixedTemplatesWithWildcardParties(
      offset: Offset,
      wildcardParties: Set[Party],
      partiesAndTemplateIds: Set[(Party, Identifier)],
      pageSize: Int,
      rowOffset: Long,
  ): SimpleSql[Row]

  final def apply(
      offset: Offset,
      filter: FilterRelation,
      pageSize: Int,
      rowOffset: Long,
  ): SimpleSql[Row] = {
    require(filter.nonEmpty, "The request must be issued by at least one party")

    // Route the request to the correct underlying query
    // 1. If no party requests specific template identifiers
    val parties = filter.keySet
    if (filter.forall(_._2.isEmpty))
      onlyWildcardParties(
        offset = offset,
        parties = parties,
        pageSize = pageSize,
        rowOffset = rowOffset,
      )
    else {
      // 2. If all parties request the same template identifier
      val templateIds = filter.valuesIterator.flatten.toSet
      if (filter.valuesIterator.forall(_ == templateIds)) {
        sameTemplates(
          offset,
          parties = parties,
          templateIds = templateIds,
          pageSize = pageSize,
          rowOffset = rowOffset,
        )
      } else {
        // 3. If there are different template identifier but there are no wildcard parties
        val partiesAndTemplateIds = FilterRelation.flatten(filter).toSet
        val wildcardParties = filter.filter(_._2.isEmpty).keySet
        if (wildcardParties.isEmpty) {
          mixedTemplates(
            offset,
            partiesAndTemplateIds = partiesAndTemplateIds,
            pageSize = pageSize,
            rowOffset = rowOffset,
          )
        } else {
          // 4. If there are wildcard parties and different template identifiers
          mixedTemplatesWithWildcardParties(
            offset,
            wildcardParties,
            partiesAndTemplateIds,
            pageSize,
            rowOffset,
          )
        }
      }
    }
  }

}

private[events] object EventsTableFlatEventsRangeQueries {

  final class GetTransactions(
      selectColumns: String,
      flatEventsTable: String,
      groupByColumns: String,
      orderByColumns: String,
  ) extends EventsTableFlatEventsRangeQueries[(Offset, Offset)] {

    protected def onlyWildcardParties(
        between: (Offset, Offset),
        parties: Set[Party],
        pageSize: Int,
        rowOffset: Long,
    ): SimpleSql[Row] =
      SQL"select #$selectColumns, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where event_offset > ${between._1} and event_offset <= ${between._2} and event_witness in ($parties) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"

    protected def sameTemplates(
        between: (Offset, Offset),
        parties: Set[Party],
        templateIds: Set[Identifier],
        pageSize: Int,
        rowOffset: Long,
    ): SimpleSql[Row] =
      SQL"select #$selectColumns, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where event_offset > ${between._1} and event_offset <= ${between._2} and event_witness in ($parties) and concat(template_package_id, ':', template_name) in ($templateIds) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"

    protected def mixedTemplates(
        between: (Offset, Offset),
        partiesAndTemplateIds: Set[(Party, Identifier)],
        pageSize: Int,
        rowOffset: Long,
    ): SimpleSql[Row] = {
      val parties = partiesAndTemplateIds.map(_._1)
      val partiesAndTemplateIdsAsString = partiesAndTemplateIds.map { case (p, i) => s"$p&$i" }
      SQL"select #$selectColumns, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where event_offset > ${between._1} and event_offset <= ${between._2} and concat(event_witness, '&', template_package_id, ':', template_name) in ($partiesAndTemplateIdsAsString) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"
    }

    protected def mixedTemplatesWithWildcardParties(
        between: (Offset, Offset),
        wildcardParties: Set[Party],
        partiesAndTemplateIds: Set[(Party, Identifier)],
        pageSize: Int,
        rowOffset: Long,
    ): SimpleSql[Row] = {
      val parties = wildcardParties ++ partiesAndTemplateIds.map(_._1)
      val partiesAndTemplateIdsAsString = partiesAndTemplateIds.map { case (p, i) => s"$p&$i" }
      SQL"select #$selectColumns, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where event_offset > ${between._1} and event_offset <= ${between._2} and (event_witness in ($wildcardParties) or concat(event_witness, '&', template_package_id, ':', template_name) in ($partiesAndTemplateIdsAsString)) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"
    }

  }

  final class GetActiveContracts(
      selectColumns: String,
      flatEventsTable: String,
      groupByColumns: String,
      orderByColumns: String,
  ) extends EventsTableFlatEventsRangeQueries[Offset] {

    def onlyWildcardParties(
        activeAt: Offset,
        parties: Set[Party],
        pageSize: Int,
        rowOffset: Long,
    ): SimpleSql[Row] =
      SQL"select #$selectColumns, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where create_argument is not null and event_offset <= $activeAt and (create_consumed_at is null or create_consumed_at > $activeAt) and event_witness in ($parties) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"

    def sameTemplates(
        activeAt: Offset,
        parties: Set[Party],
        templateIds: Set[Identifier],
        pageSize: Int,
        rowOffset: Long,
    ): SimpleSql[Row] =
      SQL"select #$selectColumns, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where create_argument is not null and event_offset <= $activeAt and (create_consumed_at is null or create_consumed_at > $activeAt) and event_witness in ($parties) and concat(template_package_id, ':', template_name) in ($templateIds) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"

    def mixedTemplates(
        activeAt: Offset,
        partiesAndTemplateIds: Set[(Party, Identifier)],
        pageSize: Int,
        rowOffset: Long,
    ): SimpleSql[Row] = {
      val parties = partiesAndTemplateIds.map(_._1)
      val partiesAndTemplateIdsAsString = partiesAndTemplateIds.map { case (p, i) => s"$p&$i" }
      SQL"select #$selectColumns, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where create_argument is not null and event_offset <= $activeAt and (create_consumed_at is null or create_consumed_at > $activeAt) and concat(event_witness, '&', template_package_id, ':', template_name) in ($partiesAndTemplateIdsAsString) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"
    }

    def mixedTemplatesWithWildcardParties(
        activeAt: Offset,
        wildcardParties: Set[Party],
        partiesAndTemplateIds: Set[(Party, Identifier)],
        pageSize: Int,
        rowOffset: Long,
    ): SimpleSql[Row] = {
      val parties = wildcardParties ++ partiesAndTemplateIds.map(_._1)
      val partiesAndTemplateIdsAsString = partiesAndTemplateIds.map { case (p, i) => s"$p&$i" }
      SQL"select #$selectColumns, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where create_argument is not null and event_offset <= $activeAt and (create_consumed_at is null or create_consumed_at > $activeAt) and (event_witness in ($wildcardParties) or concat(event_witness, '&', template_package_id, ':', template_name) in ($partiesAndTemplateIdsAsString)) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"
    }

  }

}
