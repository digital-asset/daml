// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.{Offset, TransactionId}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse
}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.ApiOffset
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.{DbDispatcher, PaginatingAsyncStream}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf

import scala.concurrent.{ExecutionContext, Future}

/**
  * @param dispatcher Executes the queries prepared by this object
  * @param executionContext Runs transformations on data fetched from the database, including DAML-LF value deserialization
  * @param pageSize The number of events to fetch at a time the database when serving streaming calls
  * @param lfValueTranslation The delegate in charge of translating serialized DAML-LF values
  * @see [[PaginatingAsyncStream]]
  */
private[dao] final class TransactionsReader(
    dispatcher: DbDispatcher,
    dbType: DbType,
    pageSize: Int,
    metrics: Metrics,
    lfValueTranslation: LfValueTranslation,
)(implicit executionContext: ExecutionContext) {

  private val dbMetrics = metrics.daml.index.db

  private val sqlFunctions = SqlFunctions(dbType)

  private def offsetFor(response: GetTransactionsResponse): Offset =
    ApiOffset.assertFromString(response.transactions.head.offset)

  private def offsetFor(response: GetTransactionTreesResponse): Offset =
    ApiOffset.assertFromString(response.transactions.head.offset)

  private def deserializeEvent[E](verbose: Boolean)(entry: EventsTable.Entry[Raw[E]]): Future[E] =
    Future(entry.event.applyDeserialization(lfValueTranslation, verbose))

  private def deserializeEntry[E](verbose: Boolean)(
      entry: EventsTable.Entry[Raw[E]],
  ): Future[EventsTable.Entry[E]] =
    deserializeEvent(verbose)(entry).map(event => entry.copy(event = event))

  def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FilterRelation,
      verbose: Boolean,
  ): Source[(Offset, GetTransactionsResponse), NotUsed] = {

    val eventsRangeF: Future[EventsRange[Long]] = dispatcher.executeSql(dbMetrics.getRowIdRange) {
      connection =>
        EventsRange.eventsRange(EventsRange(startExclusive, endInclusive))(connection)
    }

    def getEvents(eventsRange0: EventsRange[Long]): Source[EventsTable.Entry[Event], NotUsed] =
      PaginatingAsyncStream.streamFrom(
        eventsRange0,
        getEventsRange[Event](eventsRange0.endInclusive)) { eventsRange1 =>
        val query =
          EventsTable
            .preparePagedGetFlatTransactions(sqlFunctions)(
              range = eventsRange1,
              filter = filter,
              pageSize = pageSize,
            )
            .withFetchSize(Some(pageSize))
        val rawEvents =
          dispatcher.executeSql(dbMetrics.getFlatTransactions) { implicit connection =>
            query.asVectorOf(EventsTable.rawFlatEventParser)
          }
        rawEvents.flatMap(
          es =>
            Timed.future(
              future = Future.traverse(es)(deserializeEntry(verbose)),
              timer = dbMetrics.getTransactionTrees.translationTimer,
          )
        )
      }

    val events: Source[EventsTable.Entry[Event], NotUsed] =
      Source.future(eventsRangeF).flatMapConcat(r => getEvents(r))

    groupContiguous(events)(by = _.transactionId)
      .mapConcat { events =>
        val response = EventsTable.Entry.toGetTransactionsResponse(events)
        response.map(r => offsetFor(r) -> r)
      }
  }

  private def getOffset[E](a: EventsTable.Entry[E]): (Offset, Option[Int]) =
    (a.eventOffset, Some(a.nodeIndex))

  private def getEventsRange[E](endRowId: Long)(a: EventsTable.Entry[E]): EventsRange[Long] =
    EventsRange(startExclusive = a.rowId, endInclusive = endRowId)

  def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetFlatTransactionResponse]] = {
    val query =
      EventsTable.prepareLookupFlatTransactionById(sqlFunctions)(transactionId, requestingParties)
    dispatcher
      .executeSql(
        databaseMetrics = dbMetrics.lookupFlatTransactionById,
        extraLog = Some(s"tx: $transactionId, parties = ${requestingParties.mkString(", ")}"),
      ) { implicit connection =>
        query.asVectorOf(EventsTable.rawFlatEventParser)
      }
      .flatMap(
        rawEvents =>
          Timed.value(
            timer = dbMetrics.lookupFlatTransactionById.translationTimer,
            value = Future.traverse(rawEvents)(deserializeEntry(verbose = true))
        ))
      .map(EventsTable.Entry.toGetFlatTransactionResponse)
  }

  def getTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      verbose: Boolean,
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] = {

    val eventsRangeF: Future[EventsRange[Long]] = dispatcher.executeSql(dbMetrics.getRowIdRange) {
      connection =>
        EventsRange.eventsRange(EventsRange(startExclusive, endInclusive))(connection)
    }

    def getEvents(eventsRange0: EventsRange[Long]): Source[EventsTable.Entry[TreeEvent], NotUsed] =
      PaginatingAsyncStream.streamFrom(
        eventsRange0,
        getEventsRange[TreeEvent](eventsRange0.endInclusive)) { eventsRange1 =>
        val query =
          EventsTable
            .preparePagedGetTransactionTrees(sqlFunctions)(
              eventsRange = eventsRange1,
              requestingParties = requestingParties,
              pageSize = pageSize,
            )
            .withFetchSize(Some(pageSize))
        val rawEvents =
          dispatcher.executeSql(dbMetrics.getTransactionTrees) { implicit connection =>
            query.asVectorOf(EventsTable.rawTreeEventParser)
          }
        rawEvents.flatMap(
          es =>
            Timed.future(
              future = Future.traverse(es)(deserializeEntry(verbose)),
              timer = dbMetrics.getTransactionTrees.translationTimer,
          )
        )
      }

    val events: Source[EventsTable.Entry[TreeEvent], NotUsed] =
      Source.future(eventsRangeF).flatMapConcat(r => getEvents(r))

    groupContiguous(events)(by = _.transactionId)
      .flatMapConcat { events =>
        val response = EventsTable.Entry.toGetTransactionTreesResponse(events)
        Source(response.map(r => offsetFor(r) -> r))
      }
  }

  def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetTransactionResponse]] = {
    val query =
      EventsTable.prepareLookupTransactionTreeById(sqlFunctions)(transactionId, requestingParties)
    dispatcher
      .executeSql(
        databaseMetrics = dbMetrics.lookupTransactionTreeById,
        extraLog = Some(s"tx: $transactionId, parties = ${requestingParties.mkString(", ")}"),
      ) { implicit connection =>
        query.asVectorOf(EventsTable.rawTreeEventParser)
      }
      .flatMap(
        rawEvents =>
          Timed.value(
            timer = dbMetrics.lookupTransactionTreeById.translationTimer,
            value = Future.traverse(rawEvents)(deserializeEntry(verbose = true))
        ))
      .map(EventsTable.Entry.toGetTransactionResponse)
  }

  def getActiveContracts(
      activeAt: Offset,
      filter: FilterRelation,
      verbose: Boolean,
  ): Source[GetActiveContractsResponse, NotUsed] = {
    val events =
      PaginatingAsyncStream.streamFrom((Offset.beforeBegin, Option.empty[Int]), getOffset[Event]) {
        case (prevOffset, prevNodeIndex) =>
          val query =
            EventsTable
              .preparePagedGetActiveContracts(sqlFunctions)(
                lastOffsetFromPrevPage = prevOffset,
                activeAt = activeAt,
                filter = filter,
                pageSize = pageSize,
                lastEventNodeIndexFromPrevPage = prevNodeIndex,
              )
              .withFetchSize(Some(pageSize))
          val rawEvents =
            dispatcher.executeSql(dbMetrics.getActiveContracts) { implicit connection =>
              query.asVectorOf(EventsTable.rawFlatEventParser)
            }
          Timed.future(
            future = rawEvents.flatMap(Future.traverse(_)(deserializeEntry(verbose))),
            timer = dbMetrics.getActiveContracts.translationTimer,
          )
      }

    groupContiguous(events)(by = _.transactionId)
      .flatMapConcat { events =>
        Source(EventsTable.Entry.toGetActiveContractsResponse(events))
      }
  }

}
