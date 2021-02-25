// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, SimpleSql, SqlStringInterpolation}
import com.daml.lf.data.Ref.{Identifier => ApiIdentifier}
import com.daml.platform.store.Conversions._

private[events] sealed abstract class EventsTableFlatEventsRangeQueries[Offset] {

  import EventsTableFlatEventsRangeQueries.QueryParts

  protected def singleWildcardParty(
      offset: Offset,
      party: Party,
      pageSize: Int,
  ): QueryParts

  protected def singlePartyWithTemplates(
      offset: Offset,
      party: Party,
      templateIds: Set[ApiIdentifier],
      pageSize: Int,
  ): QueryParts

  protected def onlyWildcardParties(
      offset: Offset,
      parties: Set[Party],
      pageSize: Int,
  ): QueryParts

  protected def sameTemplates(
      offset: Offset,
      parties: Set[Party],
      templateIds: Set[ApiIdentifier],
      pageSize: Int,
  ): QueryParts

  protected def mixedTemplates(
      offset: Offset,
      partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
      pageSize: Int,
  ): QueryParts

  protected def mixedTemplatesWithWildcardParties(
      offset: Offset,
      wildcardParties: Set[Party],
      partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
      pageSize: Int,
  ): QueryParts

  protected def offsetRange(offset: Offset): EventsRange[Long]

  final def apply(
      offset: Offset,
      filter: FilterRelation,
      pageSize: Int,
  ): SqlSequence[Vector[EventsTable.Entry[Raw.FlatEvent]]] = {
    require(filter.nonEmpty, "The request must be issued by at least one party")

    // Route the request to the correct underlying query
    val frqK = if (filter.size == 1) {
      val (party, templateIds) = filter.iterator.next()
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

    frqK match {
      case QueryParts.ByArith(read) =>
        EventsRange.readPage(
          read,
          EventsTableFlatEvents.rawFlatEventParser,
          offsetRange(offset),
          pageSize,
        )
      case QueryParts.ByLimit(sql) =>
        SqlSequence.vector(
          sql withFetchSize Some(pageSize),
          EventsTableFlatEvents.rawFlatEventParser,
        )
    }
  }
}

private[events] object EventsTableFlatEventsRangeQueries {

  import com.daml.ledger.participant.state.v1.Offset

  private[EventsTableFlatEventsRangeQueries] sealed abstract class QueryParts
      extends Product
      with Serializable
  private[EventsTableFlatEventsRangeQueries] object QueryParts {
    final case class ByArith(read: (EventsRange[Long], String) => SimpleSql[Row]) extends QueryParts
    final case class ByLimit(saferRead: SimpleSql[Row]) extends QueryParts
    import language.implicitConversions
    implicit def `go by limit`(saferRead: SimpleSql[Row]): ByLimit = ByLimit(saferRead)
  }

  final class GetTransactions(
      selectColumns: String,
      sqlFunctions: SqlFunctions,
  ) extends EventsTableFlatEventsRangeQueries[EventsRange[Long]] {

    override protected def singleWildcardParty(
        range: EventsRange[Long],
        party: Party,
        pageSize: Int,
    ): QueryParts = {
      val witnessesWhereClause =
        sqlFunctions.arrayIntersectionWhereClause("flat_event_witnesses", party)
      QueryParts.ByArith(
        read = (range, limitExpr) => SQL"""
            select #$selectColumns, array[$party] as event_witnesses,
                   case when submitters = array[$party]::text[] then command_id else '' end as command_id
            from participant_events
            where event_sequential_id > ${range.startExclusive}
                  and event_sequential_id <= ${range.endInclusive}
                  and #$witnessesWhereClause
            order by event_sequential_id #$limitExpr"""
      )
    }

    override protected def singlePartyWithTemplates(
        range: EventsRange[Long],
        party: Party,
        templateIds: Set[ApiIdentifier],
        pageSize: Int,
    ): QueryParts = {
      val witnessesWhereClause =
        sqlFunctions.arrayIntersectionWhereClause("flat_event_witnesses", party)
      QueryParts.ByArith(
        read = (range, limitExpr) => SQL"""
            select #$selectColumns, array[$party] as event_witnesses,
                   case when submitters = array[$party]::text[] then command_id else '' end as command_id
            from participant_events
            where event_sequential_id > ${range.startExclusive}
                  and event_sequential_id <= ${range.endInclusive}
                  and #$witnessesWhereClause
                  and template_id in ($templateIds)
            order by event_sequential_id #$limitExpr"""
      )
    }

    override protected def onlyWildcardParties(
        range: EventsRange[Long],
        parties: Set[Party],
        pageSize: Int,
    ): QueryParts = {
      val witnessesWhereClause =
        sqlFunctions.arrayIntersectionWhereClause("flat_event_witnesses", parties)
      val filteredWitnesses =
        sqlFunctions.arrayIntersectionValues("flat_event_witnesses", parties)
      val submittersInPartiesClause =
        sqlFunctions.arrayIntersectionWhereClause("submitters", parties)
      QueryParts.ByArith(
        read = (range, limitExpr) => SQL"""
            select #$selectColumns, #$filteredWitnesses as event_witnesses,
                   case when #$submittersInPartiesClause then command_id else '' end as command_id
            from participant_events
            where event_sequential_id > ${range.startExclusive}
                  and event_sequential_id <= ${range.endInclusive}
                  and #$witnessesWhereClause
            order by event_sequential_id #$limitExpr"""
      )
    }

    override protected def sameTemplates(
        range: EventsRange[Long],
        parties: Set[Party],
        templateIds: Set[ApiIdentifier],
        pageSize: Int,
    ): QueryParts = {
      val witnessesWhereClause =
        sqlFunctions.arrayIntersectionWhereClause("flat_event_witnesses", parties)
      val filteredWitnesses =
        sqlFunctions.arrayIntersectionValues("flat_event_witnesses", parties)
      val submittersInPartiesClause =
        sqlFunctions.arrayIntersectionWhereClause("submitters", parties)
      QueryParts.ByArith(
        read = (range, limitExpr) => SQL"""
            select #$selectColumns, #$filteredWitnesses as event_witnesses,
                   case when #$submittersInPartiesClause then command_id else '' end as command_id
            from participant_events
            where event_sequential_id > ${range.startExclusive}
                  and event_sequential_id <= ${range.endInclusive}
                  and #$witnessesWhereClause
                  and template_id in ($templateIds)
            order by event_sequential_id #$limitExpr"""
      )
    }

    override protected def mixedTemplates(
        range: EventsRange[Long],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
        pageSize: Int,
    ): QueryParts = {
      val parties = partiesAndTemplateIds.map(_._1)
      val partiesAndTemplatesCondition =
        formatPartiesAndTemplatesWhereClause(
          sqlFunctions,
          "flat_event_witnesses",
          partiesAndTemplateIds,
        )
      val filteredWitnesses =
        sqlFunctions.arrayIntersectionValues("flat_event_witnesses", parties)
      val submittersInPartiesClause =
        sqlFunctions.arrayIntersectionWhereClause("submitters", parties)
      QueryParts.ByArith(
        read = (range, limitExpr) => SQL"""
            select #$selectColumns, #$filteredWitnesses as event_witnesses,
                   case when #$submittersInPartiesClause then command_id else '' end as command_id
            from participant_events
            where event_sequential_id > ${range.startExclusive}
                  and event_sequential_id <= ${range.endInclusive}
                  and #$partiesAndTemplatesCondition
            order by event_sequential_id #$limitExpr"""
      )
    }

    override protected def mixedTemplatesWithWildcardParties(
        range: EventsRange[Long],
        wildcardParties: Set[Party],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
        pageSize: Int,
    ): QueryParts = {
      val parties = wildcardParties ++ partiesAndTemplateIds.map(_._1)
      val partiesAndTemplatesCondition =
        formatPartiesAndTemplatesWhereClause(
          sqlFunctions,
          "flat_event_witnesses",
          partiesAndTemplateIds,
        )
      val witnessesWhereClause =
        sqlFunctions.arrayIntersectionWhereClause("flat_event_witnesses", wildcardParties)
      val filteredWitnesses =
        sqlFunctions.arrayIntersectionValues("flat_event_witnesses", parties)
      val submittersInPartiesClause =
        sqlFunctions.arrayIntersectionWhereClause("submitters", parties)
      QueryParts.ByArith(
        read = (range, limitExpr) => SQL"""
            select #$selectColumns, #$filteredWitnesses as event_witnesses,
                   case when #$submittersInPartiesClause then command_id else '' end as command_id
            from participant_events
            where event_sequential_id > ${range.startExclusive}
                  and event_sequential_id <= ${range.endInclusive}
                  and (#$witnessesWhereClause or #$partiesAndTemplatesCondition)
            order by event_sequential_id #$limitExpr"""
      )
    }

    override protected def offsetRange(offset: EventsRange[Long]) = offset
  }

  final class GetActiveContracts(
      selectColumns: String,
      sqlFunctions: SqlFunctions,
  ) extends EventsTableFlatEventsRangeQueries[EventsRange[(Offset, Long)]] {

    override protected def singleWildcardParty(
        range: EventsRange[(Offset, Long)],
        party: Party,
        pageSize: Int,
    ): QueryParts = {
      def witnessesWhereClause(prefix: String) =
        sqlFunctions.arrayIntersectionWhereClause(s"$prefix.flat_event_witnesses", party)
      SQL"""select #$selectColumns, array[$party] as event_witnesses,
                   case when active_cs.submitters = array[$party]::text[] then active_cs.command_id else '' end as command_id
            from participant_events as active_cs
            where active_cs.event_kind = 10 -- create
                  and active_cs.event_sequential_id > ${range.startExclusive._2: Long}
                  and active_cs.event_sequential_id <= ${range.endInclusive._2: Long}
                  and not exists (
                    select 1
                    from participant_events as archived_cs
                    where
                      archived_cs.contract_id = active_cs.contract_id and
                      archived_cs.event_kind = 20 and -- consuming
                      archived_cs.event_offset <= ${range.endInclusive._1: Offset}
                  )
                  and #${witnessesWhereClause("active_cs")}
            order by active_cs.event_sequential_id limit $pageSize"""
    }

    override protected def singlePartyWithTemplates(
        range: EventsRange[(Offset, Long)],
        party: Party,
        templateIds: Set[ApiIdentifier],
        pageSize: Int,
    ): QueryParts = {
      val witnessesWhereClause =
        sqlFunctions.arrayIntersectionWhereClause("active_cs.flat_event_witnesses", party)
      SQL"""select #$selectColumns, array[$party] as event_witnesses,
                   case when active_cs.submitters = array[$party]::text[] then active_cs.command_id else '' end as command_id
            from participant_events as active_cs
            where active_cs.event_kind = 10 -- create
                  and active_cs.event_sequential_id > ${range.startExclusive._2: Long}
                  and active_cs.event_sequential_id <= ${range.endInclusive._2: Long}
                  and not exists (
                    select 1
                    from participant_events as archived_cs
                    where
                      archived_cs.contract_id = active_cs.contract_id and
                      archived_cs.event_kind = 20 and -- consuming and
                      archived_cs.event_offset <= ${range.endInclusive._1: Offset}
                  )
                  and #$witnessesWhereClause
                  and active_cs.template_id in ($templateIds)
            order by active_cs.event_sequential_id limit $pageSize"""
    }

    override def onlyWildcardParties(
        range: EventsRange[(Offset, Long)],
        parties: Set[Party],
        pageSize: Int,
    ): QueryParts = {
      val witnessesWhereClause =
        sqlFunctions.arrayIntersectionWhereClause("active_cs.flat_event_witnesses", parties)
      val filteredWitnesses =
        sqlFunctions.arrayIntersectionValues("active_cs.flat_event_witnesses", parties)
      val submittersInPartiesClause =
        sqlFunctions.arrayIntersectionWhereClause("active_cs.submitters", parties)
      SQL"""select #$selectColumns, #$filteredWitnesses as event_witnesses,
                   case when #$submittersInPartiesClause then active_cs.command_id else '' end as command_id
            from participant_events as active_cs
            where active_cs.event_kind = 10 -- create
                  and active_cs.event_sequential_id > ${range.startExclusive._2: Long}
                  and active_cs.event_sequential_id <= ${range.endInclusive._2: Long}
                  and not exists (
                    select 1
                    from participant_events as archived_cs
                    where
                      archived_cs.contract_id = active_cs.contract_id and
                      archived_cs.event_kind = 20 and -- consuming and
                      archived_cs.event_offset <= ${range.endInclusive._1: Offset}
                  )
                  and #$witnessesWhereClause
            order by active_cs.event_sequential_id limit $pageSize"""
    }

    override def sameTemplates(
        range: EventsRange[(Offset, Long)],
        parties: Set[Party],
        templateIds: Set[ApiIdentifier],
        pageSize: Int,
    ): QueryParts = {
      val witnessesWhereClause =
        sqlFunctions.arrayIntersectionWhereClause("active_cs.flat_event_witnesses", parties)
      val filteredWitnesses =
        sqlFunctions.arrayIntersectionValues("active_cs.flat_event_witnesses", parties)
      val submittersInPartiesClause =
        sqlFunctions.arrayIntersectionWhereClause("active_cs.submitters", parties)
      SQL"""select #$selectColumns, #$filteredWitnesses as event_witnesses,
                   case when #$submittersInPartiesClause then active_cs.command_id else '' end as command_id
            from participant_events as active_cs
            where active_cs.event_kind = 10 -- create
                  and active_cs.event_sequential_id > ${range.startExclusive._2: Long}
                  and active_cs.event_sequential_id <= ${range.endInclusive._2: Long}
                  and not exists (
                    select 1
                    from participant_events as archived_cs
                    where
                      archived_cs.contract_id = active_cs.contract_id and
                      archived_cs.event_kind = 20 and -- consuming and
                      archived_cs.event_offset <= ${range.endInclusive._1: Offset}
                  )
                  and #$witnessesWhereClause
                  and active_cs.template_id in ($templateIds)
            order by active_cs.event_sequential_id limit $pageSize"""
    }

    override def mixedTemplates(
        range: EventsRange[(Offset, Long)],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
        pageSize: Int,
    ): QueryParts = {
      val parties = partiesAndTemplateIds.map(_._1)
      val partiesAndTemplatesCondition =
        formatPartiesAndTemplatesWhereClause(
          sqlFunctions,
          "active_cs.flat_event_witnesses",
          partiesAndTemplateIds,
        )
      val filteredWitnesses =
        sqlFunctions.arrayIntersectionValues("active_cs.flat_event_witnesses", parties)
      val submittersInPartiesClause =
        sqlFunctions.arrayIntersectionWhereClause("active_cs.submitters", parties)
      // TODO these repetitions are painful to maintain. Ideas: Adding SQL views (do reuse in SQL schema), pursue anorm to enable reuse, using something else then anorm here.
      SQL"""select #$selectColumns, #$filteredWitnesses as event_witnesses,
                   case when #$submittersInPartiesClause then active_cs.command_id else '' end as command_id
            from participant_events as active_cs
            where active_cs.event_kind = 10 -- create
                  and active_cs.event_sequential_id > ${range.startExclusive._2: Long}
                  and active_cs.event_sequential_id <= ${range.endInclusive._2: Long}
                  and not exists (
                    select 1
                    from participant_events as archived_cs
                    where
                      archived_cs.contract_id = active_cs.contract_id and
                      archived_cs.event_kind = 20 and -- consuming and
                      archived_cs.event_offset <= ${range.endInclusive._1: Offset}
                  )
                  and #$partiesAndTemplatesCondition
            order by active_cs.event_sequential_id limit $pageSize"""
    }

    override def mixedTemplatesWithWildcardParties(
        range: EventsRange[(Offset, Long)],
        wildcardParties: Set[Party],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
        pageSize: Int,
    ): QueryParts = {
      val parties = wildcardParties ++ partiesAndTemplateIds.map(_._1)
      val partiesAndTemplatesCondition =
        formatPartiesAndTemplatesWhereClause(
          sqlFunctions,
          "flat_event_witnesses",
          partiesAndTemplateIds,
        )
      val witnessesWhereClause =
        sqlFunctions.arrayIntersectionWhereClause("active_cs.flat_event_witnesses", wildcardParties)
      val filteredWitnesses =
        sqlFunctions.arrayIntersectionValues("active_cs.flat_event_witnesses", parties)
      val submittersInPartiesClause =
        sqlFunctions.arrayIntersectionWhereClause("active_cs.submitters", parties)
      SQL"""select #$selectColumns, #$filteredWitnesses as event_witnesses,
                   case when #$submittersInPartiesClause then active_cs.command_id else '' end as command_id
            from participant_events as active_cs
            where active_cs.event_kind = 10 -- create
                  and active_cs.event_sequential_id > ${range.startExclusive._2: Long}
                  and active_cs.event_sequential_id <= ${range.endInclusive._2: Long}
                  and not exists (
                    select 1
                    from participant_events as archived_cs
                    where
                      archived_cs.contract_id = active_cs.contract_id and
                      archived_cs.event_kind = 20 and -- consuming and
                      archived_cs.event_offset <= ${range.endInclusive._1: Offset}
                  )
                  and (#$witnessesWhereClause or #$partiesAndTemplatesCondition)
            order by active_cs.event_sequential_id limit $pageSize"""
    }

    override protected def offsetRange(offset: EventsRange[(Offset, Long)]) = offset map (_._2)
  }

  private def formatPartiesAndTemplatesWhereClause(
      sqlFunctions: SqlFunctions,
      witnessesAggregationColumn: String,
      partiesAndTemplateIds: Set[(Party, Identifier)],
  ): String =
    partiesAndTemplateIds.view
      .map { case (p, i) =>
        s"(${sqlFunctions.arrayIntersectionWhereClause(witnessesAggregationColumn, p)} and template_id = '$i')"
      }
      .mkString("(", " or ", ")")
}
