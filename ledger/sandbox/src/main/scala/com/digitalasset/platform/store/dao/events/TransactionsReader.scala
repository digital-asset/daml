// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection

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
import com.daml.metrics.{DatabaseMetrics, Metrics, Timed}
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

    val requestedRangeF = getEventSeqIdRange(startExclusive, endInclusive)

    val query = (range: EventsRange[Long]) =>
      (connection: Connection) =>
        EventsTable
          .preparePagedGetFlatTransactions(sqlFunctions)(
            range = range,
            filter = filter,
            pageSize = pageSize,
          )
          .withFetchSize(Some(pageSize))
          .asVectorOf(EventsTable.rawFlatEventParser)(connection)

    val events: Source[EventsTable.Entry[Event], NotUsed] =
      Source
        .futureSource(requestedRangeF.map { requestedRange =>
          streamEvents(
            verbose,
            dbMetrics.getFlatTransactions,
            query,
            nextPageRange[Event](requestedRange.endInclusive)
          )(requestedRange)
        })
        .mapMaterializedValue(_ => NotUsed)

    groupContiguous(events)(by = _.transactionId)
      .mapConcat { events =>
        val response = EventsTable.Entry.toGetTransactionsResponse(events)
        response.map(r => offsetFor(r) -> r)
      }
  }

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

    val requestedRangeF = getEventSeqIdRange(startExclusive, endInclusive)

    val query = (range: EventsRange[Long]) =>
      (connection: Connection) =>
        EventsTable
          .preparePagedGetTransactionTrees(sqlFunctions)(
            eventsRange = range,
            requestingParties = requestingParties,
            pageSize = pageSize,
          )
          .withFetchSize(Some(pageSize))
          .asVectorOf(EventsTable.rawTreeEventParser)(connection)

    val events: Source[EventsTable.Entry[TreeEvent], NotUsed] =
      Source
        .futureSource(requestedRangeF.map { requestedRange =>
          streamEvents(
            verbose,
            dbMetrics.getTransactionTrees,
            query,
            nextPageRange[TreeEvent](requestedRange.endInclusive)
          )(requestedRange)
        })
        .mapMaterializedValue(_ => NotUsed)

    groupContiguous(events)(by = _.transactionId)
      .mapConcat { events =>
        val response = EventsTable.Entry.toGetTransactionTreesResponse(events)
        response.map(r => offsetFor(r) -> r)
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
      ) { connection =>
        query.asVectorOf(EventsTable.rawTreeEventParser)(connection)
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

    val requestedRangeF: Future[EventsRange[(Offset, Long)]] = getAcsEventSeqIdRange(activeAt)

    val query = (range: EventsRange[(Offset, Long)]) =>
      (connection: Connection) =>
        EventsTable
          .preparePagedGetActiveContracts(sqlFunctions)(
            range = range,
            filter = filter,
            pageSize = pageSize,
          )
          .withFetchSize(Some(pageSize))
          .asVectorOf(EventsTable.rawFlatEventParser)(connection)

    val events: Source[EventsTable.Entry[Event], NotUsed] =
      Source
        .futureSource(requestedRangeF.map { requestedRange =>
          streamEvents(
            verbose,
            dbMetrics.getActiveContracts,
            query,
            nextPageRange[Event](requestedRange.endInclusive)
          )(requestedRange)
        })
        .mapMaterializedValue(_ => NotUsed)

    groupContiguous(events)(by = _.transactionId)
      .mapConcat(EventsTable.Entry.toGetActiveContractsResponse)
  }

  private def nextPageRange[E](endEventSeqId: Long)(a: EventsTable.Entry[E]): EventsRange[Long] =
    EventsRange(startExclusive = a.eventSequentialId, endInclusive = endEventSeqId)

  private def nextPageRange[E](endEventSeqId: (Offset, Long))(
      a: EventsTable.Entry[E]): EventsRange[(Offset, Long)] =
    EventsRange(startExclusive = (a.eventOffset, a.eventSequentialId), endInclusive = endEventSeqId)

  private def getAcsEventSeqIdRange(activeAt: Offset): Future[EventsRange[(Offset, Long)]] =
    dispatcher
      .executeSql(dbMetrics.getAcsEventSeqIdRange)(EventsRange.readEventSeqIdRange(activeAt))
      .map { x =>
        EventsRange(
          startExclusive = (Offset.beforeBegin, 0),
          endInclusive = (activeAt, x.endInclusive))
      }

  private def getEventSeqIdRange(
      startExclusive: Offset,
      endInclusive: Offset
  ): Future[EventsRange[Long]] =
    dispatcher.executeSql(dbMetrics.getEventSeqIdRange)(
      EventsRange.readEventSeqIdRange(EventsRange(startExclusive, endInclusive)))

  private def streamEvents[A, E](
      verbose: Boolean,
      queryMetric: DatabaseMetrics,
      query: EventsRange[A] => Connection => Vector[EventsTable.Entry[Raw[E]]],
      getNextPageRange: EventsTable.Entry[E] => EventsRange[A],
  )(range: EventsRange[A]): Source[EventsTable.Entry[E], NotUsed] =
    PaginatingAsyncStream.streamFrom(range, getNextPageRange) { range1 =>
      val rawEvents: Future[Vector[EventsTable.Entry[Raw[E]]]] =
        dispatcher.executeSql(queryMetric)(query(range1))
      rawEvents.flatMap(
        es =>
          Timed.future(
            future = Future.traverse(es)(deserializeEntry(verbose)),
            timer = queryMetric.translationTimer,
        )
      )
    }
}
