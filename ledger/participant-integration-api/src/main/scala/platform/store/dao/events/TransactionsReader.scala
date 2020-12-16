// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection

import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.daml
import com.daml.ledger.api.TraceIdentifiers
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse
}
import com.daml.ledger.participant.state.v1.{Offset, TransactionId}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics._
import com.daml.platform.ApiOffset
import com.daml.platform.store.DbType
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.dao.{DbDispatcher, PaginatingAsyncStream}
import io.opentelemetry.trace.Span

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

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

  private val logger = ContextualizedLogger.get(this.getClass)

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
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] = {
    val span =
      ParticipantTracer
        .spanBuilder("com.daml.platform.store.dao.events.TransactionsReader.getFlatTransactions")
        .setNoParent()
        .setAttribute(SpanAttribute.OffsetFrom.key, startExclusive.toHexString)
        .setAttribute(SpanAttribute.OffsetTo.key, endInclusive.toHexString)
        .startSpan()
    logger.debug(s"getFlatTransactions($startExclusive, $endInclusive, $filter, $verbose)")

    val requestedRangeF = getEventSeqIdRange(startExclusive, endInclusive)

    val query = (range: EventsRange[(Offset, Long)]) => { implicit connection: Connection =>
      logger.debug(s"getFlatTransactions query($range)")
      QueryNonPruned.executeSqlOrThrow(
        EventsTableFlatEvents
          .preparePagedGetFlatTransactions(sqlFunctions)(
            range = EventsRange(range.startExclusive._2, range.endInclusive._2),
            filter = filter,
            pageSize = pageSize,
          )
          .executeSql,
        range.startExclusive._1,
        pruned =>
          s"Transactions request from ${range.startExclusive._1.toHexString} to ${range.endInclusive._1.toHexString} precedes pruned offset ${pruned.toHexString}"
      )
    }

    val events: Source[EventsTable.Entry[Event], NotUsed] =
      Source
        .futureSource(requestedRangeF.map { requestedRange =>
          streamEvents(
            verbose,
            dbMetrics.getFlatTransactions,
            query,
            nextPageRange[Event](requestedRange.endInclusive),
          )(requestedRange)
        })
        .mapMaterializedValue(_ => NotUsed)

    groupContiguous(events)(by = _.transactionId)
      .mapConcat { events =>
        val response = EventsTable.Entry.toGetTransactionsResponse(events)
        response.map(r => offsetFor(r) -> r)
      }
      .wireTap(_ match {
        case (_, response) =>
          response.transactions.foreach(txn =>
            span.addEvent(daml.metrics.Event("transaction", TraceIdentifiers.fromTransaction(txn))))
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] = {
    val query =
      EventsTableFlatEvents.prepareLookupFlatTransactionById(sqlFunctions)(
        transactionId,
        requestingParties)
    dispatcher
      .executeSql(dbMetrics.lookupFlatTransactionById) { implicit connection =>
        query.asVectorOf(EventsTableFlatEvents.rawFlatEventParser)
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
  )(implicit loggingContext: LoggingContext)
    : Source[(Offset, GetTransactionTreesResponse), NotUsed] = {
    val span =
      ParticipantTracer
        .spanBuilder("com.daml.platform.store.dao.events.TransactionsReader.getTransactionTrees")
        .setNoParent()
        .setAttribute(SpanAttribute.OffsetFrom.key, startExclusive.toHexString)
        .setAttribute(SpanAttribute.OffsetTo.key, endInclusive.toHexString)
        .startSpan()
    logger.debug(
      s"getTransactionTrees($startExclusive, $endInclusive, $requestingParties, $verbose)")

    val requestedRangeF = getEventSeqIdRange(startExclusive, endInclusive)

    val query = (range: EventsRange[(Offset, Long)]) => { implicit connection: Connection =>
      logger.debug(s"getTransactionTrees query($range)")
      QueryNonPruned.executeSqlOrThrow(
        EventsTableTreeEvents
          .preparePagedGetTransactionTrees(sqlFunctions)(
            eventsRange = EventsRange(range.startExclusive._2, range.endInclusive._2),
            requestingParties = requestingParties,
            pageSize = pageSize
          )
          .executeSql,
        range.startExclusive._1,
        pruned =>
          s"Transactions request from ${range.startExclusive._1.toHexString} to ${range.endInclusive._1.toHexString} precedes pruned offset ${pruned.toHexString}"
      )
    }

    val events: Source[EventsTable.Entry[TreeEvent], NotUsed] =
      Source
        .futureSource(requestedRangeF.map { requestedRange =>
          streamEvents(
            verbose,
            dbMetrics.getTransactionTrees,
            query,
            nextPageRange[TreeEvent](requestedRange.endInclusive),
          )(requestedRange)
        })
        .mapMaterializedValue(_ => NotUsed)

    groupContiguous(events)(by = _.transactionId)
      .mapConcat { events =>
        val response = EventsTable.Entry.toGetTransactionTreesResponse(events)
        response.map(r => offsetFor(r) -> r)
      }
      .wireTap(_ match {
        case (_, response) =>
          response.transactions.foreach(txn =>
            span.addEvent(
              daml.metrics.Event("transaction", TraceIdentifiers.fromTransactionTree(txn))))
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] = {
    val query =
      EventsTableTreeEvents.prepareLookupTransactionTreeById(sqlFunctions)(
        transactionId,
        requestingParties)
    dispatcher
      .executeSql(dbMetrics.lookupTransactionTreeById) { implicit connection =>
        query.asVectorOf(EventsTableTreeEvents.rawTreeEventParser)
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
  )(implicit loggingContext: LoggingContext): Source[GetActiveContractsResponse, NotUsed] = {
    val span =
      ParticipantTracer
        .spanBuilder("com.daml.platform.store.dao.events.TransactionsReader.getActiveContracts")
        .setNoParent()
        .setAttribute(SpanAttribute.Offset.key, activeAt.toHexString)
        .startSpan()
    logger.debug(s"getActiveContracts($activeAt, $filter, $verbose)")

    val requestedRangeF: Future[EventsRange[(Offset, Long)]] = getAcsEventSeqIdRange(activeAt)

    val query = (range: EventsRange[(Offset, Long)]) => { implicit connection: Connection =>
      logger.debug(s"getActiveContracts query($range)")
      QueryNonPruned.executeSqlOrThrow(
        EventsTableFlatEvents
          .preparePagedGetActiveContracts(sqlFunctions)(
            range = range,
            filter = filter,
            pageSize = pageSize,
          )
          .executeSql,
        activeAt,
        pruned =>
          s"Active contracts request after ${activeAt.toHexString} precedes pruned offset ${pruned.toHexString}"
      )
    }

    val events: Source[EventsTable.Entry[Event], NotUsed] =
      Source
        .futureSource(requestedRangeF.map { requestedRange =>
          streamEvents(
            verbose,
            dbMetrics.getActiveContracts,
            query,
            nextPageRange[Event](requestedRange.endInclusive),
          )(requestedRange)
        })
        .mapMaterializedValue(_ => NotUsed)

    groupContiguous(events)(by = _.transactionId)
      .mapConcat(EventsTable.Entry.toGetActiveContractsResponse)
      .wireTap(response => {
        span.addEvent(
          com.daml.metrics.Event("contract", Map((SpanAttribute.Offset.key, response.offset))))
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  private def nextPageRange[E](endEventSeqId: (Offset, Long))(
      a: EventsTable.Entry[E],
  ): EventsRange[(Offset, Long)] =
    EventsRange(startExclusive = (a.eventOffset, a.eventSequentialId), endInclusive = endEventSeqId)

  private def getAcsEventSeqIdRange(activeAt: Offset)(
      implicit loggingContext: LoggingContext,
  ): Future[EventsRange[(Offset, Long)]] =
    dispatcher
      .executeSql(dbMetrics.getAcsEventSeqIdRange)(implicit connection =>
        QueryNonPruned.executeSql(
          EventsRange.readEventSeqIdRange(activeAt)(connection),
          activeAt,
          pruned =>
            s"Active contracts request after ${activeAt.toHexString} precedes pruned offset ${pruned.toHexString}"
      ))
      .flatMap(_.fold(Future.failed, Future.successful))
      .map { x =>
        EventsRange(
          startExclusive = (Offset.beforeBegin, 0),
          endInclusive = (activeAt, x.endInclusive))
      }

  private def getEventSeqIdRange(
      startExclusive: Offset,
      endInclusive: Offset
  )(implicit loggingContext: LoggingContext): Future[EventsRange[(Offset, Long)]] =
    dispatcher
      .executeSql(dbMetrics.getEventSeqIdRange)(
        implicit connection =>
          QueryNonPruned.executeSql(
            EventsRange.readEventSeqIdRange(EventsRange(startExclusive, endInclusive))(connection),
            startExclusive,
            pruned =>
              s"Transactions request from ${startExclusive.toHexString} to ${endInclusive.toHexString} precedes pruned offset ${pruned.toHexString}"
        ))
      .flatMap(
        _.fold(
          Future.failed,
          x =>
            Future.successful(
              EventsRange(
                startExclusive = (startExclusive, x.startExclusive),
                endInclusive = (endInclusive, x.endInclusive)))))

  private def streamEvents[A: Ordering, E](
      verbose: Boolean,
      queryMetric: DatabaseMetrics,
      query: EventsRange[A] => Connection => Vector[EventsTable.Entry[Raw[E]]],
      getNextPageRange: EventsTable.Entry[E] => EventsRange[A],
  )(range: EventsRange[A])(
      implicit loggingContext: LoggingContext,
  ): Source[EventsTable.Entry[E], NotUsed] =
    PaginatingAsyncStream.streamFrom(range, getNextPageRange) { range1 =>
      if (EventsRange.isEmpty(range1))
        Future.successful(Vector.empty)
      else {
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

  private def endSpanOnTermination[Mat, Out](span: Span)(mat: Mat, done: Future[Done]): Mat = {
    done.onComplete {
      case Failure(exception) =>
        span.recordException(exception)
        span.end()
      case Success(_) =>
        span.end()
    }
    mat
  }
}
