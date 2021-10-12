// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.sql.Connection

import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.backend.EventStorageBackend.{FilterParams, RangeParams}
import com.daml.platform.store.cache.StringInterning

private[events] sealed abstract class EventsTableFlatEventsRangeQueries[Offset] {

  import EventsTableFlatEventsRangeQueries.QueryParts

  protected def query(
      offset: Offset,
      filterParams: FilterParams,
  ): QueryParts

  protected def offsetRange(offset: Offset): EventsRange[Long]

  final def apply(
      offset: Offset,
      filter: FilterRelation,
      pageSize: Int,
  ): Connection => Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    require(filter.nonEmpty, "The request must be issued by at least one party")

    // Route the request to the correct underlying query
    val filterParams = if (filter.size == 1) {
      val (party, templateIds) = filter.iterator.next()
      if (templateIds.isEmpty) {
        // Single-party request, no specific template identifier
        FilterParams(
          wildCardParties = Set(party),
          partiesAndTemplates = Set.empty,
        )
      } else {
        // Single-party request, restricted to a set of template identifiers
        FilterParams(
          wildCardParties = Set.empty,
          partiesAndTemplates = Set(Set(party) -> templateIds),
        )
      }
    } else {
      // Multi-party requests
      // If no party requests specific template identifiers
      val parties = filter.keySet
      if (filter.forall(_._2.isEmpty))
        FilterParams(
          wildCardParties = parties,
          partiesAndTemplates = Set.empty,
        )
      else {
        // If all parties request the same template identifier
        val templateIds = filter.valuesIterator.flatten.toSet
        if (filter.valuesIterator.forall(_ == templateIds)) {
          FilterParams(
            wildCardParties = Set.empty,
            partiesAndTemplates = Set(parties -> templateIds),
          )
        } else {
          // The generic case: passing down in the same shape, collecting wildCardParties
          FilterParams(
            wildCardParties = filter.filter(_._2.isEmpty).keySet,
            partiesAndTemplates = filter.iterator.collect {
              case (party, templateIds) if templateIds.nonEmpty => Set(party) -> templateIds
            }.toSet,
          )
        }
      }
    }

    query(offset, filterParams) match {
      case QueryParts.ByArith(read) =>
        EventsRange.readPage(
          read,
          offsetRange(offset),
          pageSize,
        )
      case QueryParts.ByLimit(sql) =>
        sql(pageSize)
    }
  }
}

private[events] object EventsTableFlatEventsRangeQueries {

  import com.daml.ledger.offset.Offset

  private[EventsTableFlatEventsRangeQueries] sealed abstract class QueryParts
      extends Product
      with Serializable
  private[EventsTableFlatEventsRangeQueries] object QueryParts {
    final case class ByArith(
        read: (
            EventsRange[Long],
            Option[Int],
            Option[Int],
        ) => Connection => Vector[EventsTable.Entry[Raw.FlatEvent]]
    ) extends QueryParts
    final case class ByLimit(
        saferRead: Int => Connection => Vector[EventsTable.Entry[Raw.FlatEvent]]
    ) extends QueryParts
  }

  final class GetTransactions(
      storageBackend: EventStorageBackend,
      stringInterning: StringInterning,
  ) extends EventsTableFlatEventsRangeQueries[EventsRange[Long]] {

    override protected def query(
        offset: EventsRange[Long],
        filterParams: FilterParams,
    ): QueryParts =
      QueryParts.ByArith((range, limit, fetchSizeHint) =>
        storageBackend.transactionEvents(
          rangeParams = RangeParams(
            startExclusive = range.startExclusive,
            endInclusive = range.endInclusive,
            limit = limit,
            fetchSizeHint = fetchSizeHint,
          ),
          filterParams = filterParams,
          stringInterning = stringInterning,
        )
      )

    override protected def offsetRange(offset: EventsRange[Long]) = offset
  }

  final class GetActiveContracts(
      storageBackend: EventStorageBackend,
      stringInterning: StringInterning,
  ) extends EventsTableFlatEventsRangeQueries[EventsRange[(Offset, Long)]] {

    override protected def query(
        range: EventsRange[(Offset, Long)],
        filterParams: FilterParams,
    ): QueryParts =
      QueryParts.ByLimit(limit =>
        storageBackend.activeContractEvents(
          rangeParams = RangeParams(
            startExclusive = range.startExclusive._2,
            endInclusive = range.endInclusive._2,
            limit = Some(limit),
            fetchSizeHint = Some(limit),
          ),
          filterParams = filterParams,
          endInclusiveOffset = range.endInclusive._1,
          stringInterning = stringInterning,
        )
      )

    override protected def offsetRange(offset: EventsRange[(Offset, Long)]) = offset map (_._2)
  }
}
