// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
//package com.daml.platform.store.dao.events
//
//import com.daml.platform.TemplatePartiesFilter
//
//import java.sql.Connection
//import com.daml.platform.store.backend.EventStorageBackend
//import com.daml.platform.store.backend.EventStorageBackend.{FilterParams, RangeParams}
//import com.daml.platform.store.dao.events.EventsTableFlatEventsRangeQueries.filterParams
//
//// TODO pbato (2022.10.26) This was previously commented out on my POC branch
//private[events] sealed abstract class EventsTableFlatEventsRangeQueries[Offset] {
//
//  import EventsTableFlatEventsRangeQueries.QueryParts
//
//  protected def query(
//      offset: Offset,
//      filterParams: FilterParams,
//  ): QueryParts
//
//  protected def offsetRange(offset: Offset): EventsRange[Long]
//
//  final def apply(
//      offset: Offset,
//      filter: TemplatePartiesFilter,
//      pageSize: Int,
//  ): Connection => Vector[EventStorageBackend.Entry[Raw.FlatEvent]] = {
//    require(
//      filter.relation.nonEmpty || filter.wildcardParties.nonEmpty,
//      "The request must be issued by at least one party",
//    )
//
//    val parts = query(offset, filterParams(filter))
//    EventsRange.readPage(
//      parts.read,
//      offsetRange(offset),
//      pageSize,
//    )
//  }
//}
//
//private[events] object EventsTableFlatEventsRangeQueries {
//
//  // TODO pbatko: See if this can be removed since probably ETQ based streaming will use com.daml.platform.store.dao.events.FilterTableACSReader.Filter instead
//  private[events] def filterParams(
//      filter: TemplatePartiesFilter
//  ): FilterParams = if (filter.relation.size == 1) {
//    val (templateId, parties) = filter.relation.iterator.next()
//    FilterParams(
//      wildCardParties = filter.wildcardParties,
//      partiesAndTemplates = Set(parties -> Set(templateId)),
//    )
//  } else {
//    // Multi-party requests
//    // If no party requests specific template identifiers
//    FilterParams(
//      wildCardParties = filter.wildcardParties,
//      partiesAndTemplates = filter.relation.iterator.collect { case (templateId, parties) =>
//        parties -> Set(templateId)
//      }.toSet,
//    )
//  }
//
//  private[EventsTableFlatEventsRangeQueries] case class QueryParts(
//      read: (
//          EventsRange[Long],
//          Option[Int],
//          Option[Int],
//      ) => Connection => Vector[EventStorageBackend.Entry[Raw.FlatEvent]]
//  ) extends Product
//      with Serializable
//
////  final class GetTransactions(
////      storageBackend: EventStorageBackend
////  ) extends EventsTableFlatEventsRangeQueries[EventsRange[Long]] {
////
////    override protected def query(
////        offset: EventsRange[Long],
////        filterParams: FilterParams,
////    ): QueryParts =
////      QueryParts((range, limit, fetchSizeHint) =>
////        storageBackend.transactionEvents(
////          rangeParams = RangeParams(
////            startExclusive = range.startExclusive,
////            endInclusive = range.endInclusive,
////            limit = limit,
////            fetchSizeHint = fetchSizeHint,
////          ),
////          filterParams = filterParams,
////        )
////      )
////
////    override protected def offsetRange(offset: EventsRange[Long]) = offset
////  }
//}
