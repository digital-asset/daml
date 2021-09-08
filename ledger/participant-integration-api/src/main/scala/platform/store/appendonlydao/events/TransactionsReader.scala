// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.sql.Connection

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.daml.ledger.api.TraceIdentifiers
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics._
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.platform.ApiOffset
import com.daml.platform.store.appendonlydao.{DbDispatcher, PaginatingAsyncStream}
import com.daml.platform.store.backend.EventStorageBackend.{FilterParams, RangeParams}
import com.daml.platform.store.backend.StorageBackend
import com.daml.platform.store.dao.LedgerDaoTransactionsReader
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.utils.Telemetry
import com.daml.telemetry
import com.daml.telemetry.{SpanAttribute, Spans}
import io.opentelemetry.api.trace.Span

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** @param dispatcher Executes the queries prepared by this object
  * @param executionContext Runs transformations on data fetched from the database, including Daml-LF value deserialization
  * @param pageSize The number of events to fetch at a time the database when serving streaming calls
  * @param eventProcessingParallelism The parallelism for loading and decoding state events
  * @param lfValueTranslation The delegate in charge of translating serialized Daml-LF values
  * @see [[PaginatingAsyncStream]]
  */
private[appendonlydao] final class TransactionsReader(
    dispatcher: DbDispatcher,
    queryNonPruned: QueryNonPruned,
    storageBackend: StorageBackend[_],
    pageSize: Int,
    eventProcessingParallelism: Int,
    metrics: Metrics,
    lfValueTranslation: LfValueTranslation,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoTransactionsReader {

  private val dbMetrics = metrics.daml.index.db
  private val eventSeqIdReader =
    new EventsRange.EventSeqIdReader(storageBackend.maxEventSeqIdForOffset)
  private val getTransactions =
    new EventsTableFlatEventsRangeQueries.GetTransactions(storageBackend)
  private val getActiveContracts =
    new EventsTableFlatEventsRangeQueries.GetActiveContracts(storageBackend)

  private val logger = ContextualizedLogger.get(this.getClass)

  // TransactionReader adds an Akka stream buffer at the end of all streaming queries.
  // This significantly improves the performance of the transaction service.
  private val outputStreamBufferSize = 128

  private def offsetFor(response: GetTransactionsResponse): Offset =
    ApiOffset.assertFromString(response.transactions.head.offset)

  private def offsetFor(response: GetTransactionTreesResponse): Offset =
    ApiOffset.assertFromString(response.transactions.head.offset)

  private def deserializeEvent[E](verbose: Boolean)(entry: EventsTable.Entry[Raw[E]])(implicit
      loggingContext: LoggingContext
  ): Future[E] =
    entry.event.applyDeserialization(lfValueTranslation, verbose)

  private def deserializeEntry[E](verbose: Boolean)(
      entry: EventsTable.Entry[Raw[E]]
  )(implicit loggingContext: LoggingContext): Future[EventsTable.Entry[E]] =
    deserializeEvent(verbose)(entry).map(event => entry.copy(event = event))

  // TODO append-only: as consolidating event queries, consider consolidating functionality serving the flatTransactions and transactionTrees
  override def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FilterRelation,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] = {
    val span =
      Telemetry.Transactions.createSpan(startExclusive, endInclusive)(qualifiedNameOfCurrentFunc)
    logger.debug(s"getFlatTransactions($startExclusive, $endInclusive, $filter, $verbose)")

    val requestedRangeF = getEventSeqIdRange(startExclusive, endInclusive)

    val query = (range: EventsRange[(Offset, Long)]) => {
      implicit connection: Connection =>
        logger.debug(s"getFlatTransactions query($range)")
        queryNonPruned.executeSql(
          getTransactions(
            EventsRange(range.startExclusive._2, range.endInclusive._2),
            filter,
            pageSize,
          )(connection),
          range.startExclusive._1,
          pruned =>
            s"Transactions request from ${range.startExclusive._1.toHexString} to ${range.endInclusive._1.toHexString} precedes pruned offset ${pruned.toHexString}",
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

    val flatTransactionsStream = groupContiguous(events)(by = _.transactionId)
      .mapConcat { events =>
        val response = EventsTable.Entry.toGetTransactionsResponse(events)
        response.map(r => offsetFor(r) -> r)
      }

    InstrumentedSource
      .bufferedSource(
        flatTransactionsStream,
        metrics.daml.index.flatTransactionsBufferSize,
        outputStreamBufferSize,
      )
      .wireTap(_ match {
        case (_, response) =>
          response.transactions.foreach(txn =>
            Spans.addEventToSpan(
              telemetry.Event("transaction", TraceIdentifiers.fromTransaction(txn)),
              span,
            )
          )
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  override def lookupFlatTransactionById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    dispatcher
      .executeSql(dbMetrics.lookupFlatTransactionById)(
        storageBackend.flatTransaction(
          transactionId,
          FilterParams(
            wildCardParties = requestingParties,
            partiesAndTemplates = Set.empty,
          ),
        )
      )
      .flatMap(rawEvents =>
        Timed.value(
          timer = dbMetrics.lookupFlatTransactionById.translationTimer,
          value = Future.traverse(rawEvents)(deserializeEntry(verbose = true)),
        )
      )
      .map(EventsTable.Entry.toGetFlatTransactionResponse)

  override def getTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] = {
    val span =
      Telemetry.Transactions.createSpan(startExclusive, endInclusive)(qualifiedNameOfCurrentFunc)
    logger.debug(
      s"getTransactionTrees($startExclusive, $endInclusive, $requestingParties, $verbose)"
    )

    val requestedRangeF = getEventSeqIdRange(startExclusive, endInclusive)

    val query = (range: EventsRange[(Offset, Long)]) => {
      implicit connection: Connection =>
        logger.debug(s"getTransactionTrees query($range)")
        queryNonPruned.executeSql(
          EventsRange.readPage(
            read = (range, limit, fetchSizeHint) =>
              storageBackend.transactionTreeEvents(
                rangeParams = RangeParams(
                  startExclusive = range.startExclusive,
                  endInclusive = range.endInclusive,
                  limit = limit,
                  fetchSizeHint = fetchSizeHint,
                ),
                filterParams = FilterParams(
                  wildCardParties = requestingParties,
                  partiesAndTemplates = Set.empty,
                ),
              ),
            range = EventsRange(range.startExclusive._2, range.endInclusive._2),
            pageSize = pageSize,
          )(connection),
          range.startExclusive._1,
          pruned =>
            s"Transactions request from ${range.startExclusive._1.toHexString} to ${range.endInclusive._1.toHexString} precedes pruned offset ${pruned.toHexString}",
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

    val transactionTreesStream = groupContiguous(events)(by = _.transactionId)
      .mapConcat { events =>
        val response = EventsTable.Entry.toGetTransactionTreesResponse(events)
        response.map(r => offsetFor(r) -> r)
      }

    InstrumentedSource
      .bufferedSource(
        transactionTreesStream,
        metrics.daml.index.transactionTreesBufferSize,
        outputStreamBufferSize,
      )
      .wireTap(_ match {
        case (_, response) =>
          response.transactions.foreach(txn =>
            Spans.addEventToSpan(
              telemetry.Event("transaction", TraceIdentifiers.fromTransactionTree(txn)),
              span,
            )
          )
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  override def lookupTransactionTreeById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    dispatcher
      .executeSql(dbMetrics.lookupTransactionTreeById)(
        storageBackend.transactionTree(
          transactionId,
          FilterParams(
            wildCardParties = requestingParties,
            partiesAndTemplates = Set.empty,
          ),
        )
      )
      .flatMap(rawEvents =>
        Timed.value(
          timer = dbMetrics.lookupTransactionTreeById.translationTimer,
          value = Future.traverse(rawEvents)(deserializeEntry(verbose = true)),
        )
      )
      .map(EventsTable.Entry.toGetTransactionResponse)

  override def getTransactionLogUpdates(
      startExclusive: (Offset, Long),
      endInclusive: (Offset, Long),
  )(implicit
      loggingContext: LoggingContext
  ): Source[((Offset, Long), TransactionLogUpdate), NotUsed] = {
    val endMarker = Source.single(
      endInclusive -> TransactionLogUpdate.LedgerEndMarker(
        eventOffset = endInclusive._1,
        eventSequentialId = endInclusive._2,
      )
    )

    val eventsSource = Source
      .fromIterator(() =>
        TransactionsReader
          .splitRange(
            startExclusive._2,
            endInclusive._2,
            eventProcessingParallelism,
            pageSize,
          )
          .iterator
      )
      .map { range =>
        metrics.daml.services.index.getTransactionLogUpdatesChunkSize.update(
          range.endInclusive - range.startExclusive
        )
        range
      }
      // Dispatch database fetches in parallel
      .mapAsync(eventProcessingParallelism) { range =>
        dispatcher.executeSql(dbMetrics.getTransactionLogUpdates) { implicit conn =>
          queryNonPruned.executeSql(
            query = storageBackend.rawEvents(
              startExclusive = range.startExclusive,
              endInclusive = range.endInclusive,
            )(conn),
            minOffsetExclusive = startExclusive._1,
            error = pruned =>
              s"Transaction log updates request after ${startExclusive._1.toHexString} precedes pruned offset ${pruned.toHexString}",
          )
        }
      }
      .mapConcat(identity)
      .async
      // Decode transaction log updates in parallel
      .mapAsync(eventProcessingParallelism) { raw =>
        Timed.future(
          metrics.daml.index.decodeTransactionLogUpdate,
          Future(TransactionLogUpdatesReader.toTransactionEvent(raw)),
        )
      }

    val transactionLogUpdatesSource = groupContiguous(eventsSource)(by = _.transactionId)
      .map { v =>
        val tx = toTransaction(v)
        (tx.offset, tx.events.last.eventSequentialId) -> tx
      }
      .mapMaterializedValue(_ => NotUsed)

    InstrumentedSource
      .bufferedSource(
        original = transactionLogUpdatesSource,
        counter = metrics.daml.index.transactionLogUpdatesBufferSize,
        size = outputStreamBufferSize,
      )
      .concat(endMarker)
  }

  private def toTransaction(
      events: Vector[TransactionLogUpdate.Event]
  ): TransactionLogUpdate.Transaction = {
    val first = events.head
    TransactionLogUpdate.Transaction(
      transactionId = first.transactionId,
      workflowId = first.workflowId,
      effectiveAt = first.ledgerEffectiveTime,
      offset = first.eventOffset,
      events = events,
    )
  }

  override def getActiveContracts(
      activeAt: Offset,
      filter: FilterRelation,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetActiveContractsResponse, NotUsed] = {
    val span =
      Telemetry.Transactions.createSpan(activeAt)(qualifiedNameOfCurrentFunc)
    logger.debug(s"getActiveContracts($activeAt, $filter, $verbose)")

    val requestedRangeF: Future[EventsRange[(Offset, Long)]] = getAcsEventSeqIdRange(activeAt)

    val query = (range: EventsRange[(Offset, Long)]) => {
      implicit connection: Connection =>
        logger.debug(s"getActiveContracts query($range)")
        queryNonPruned.executeSql(
          getActiveContracts(
            range,
            filter,
            pageSize,
          )(connection),
          activeAt,
          pruned =>
            s"Active contracts request after ${activeAt.toHexString} precedes pruned offset ${pruned.toHexString}",
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
      .buffer(outputStreamBufferSize, OverflowStrategy.backpressure)
      .wireTap(response => {
        Spans.addEventToSpan(
          telemetry.Event("contract", Map((SpanAttribute.Offset, response.offset))),
          span,
        )
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  override def getContractStateEvents(startExclusive: (Offset, Long), endInclusive: (Offset, Long))(
      implicit loggingContext: LoggingContext
  ): Source[((Offset, Long), ContractStateEvent), NotUsed] = {

    val endMarker = Source.single(
      endInclusive -> ContractStateEvent.LedgerEndMarker(
        eventOffset = endInclusive._1,
        eventSequentialId = endInclusive._2,
      )
    )

    val contractStateEventsSource = Source
      .fromIterator(() =>
        TransactionsReader
          .splitRange(
            startExclusive._2,
            endInclusive._2,
            eventProcessingParallelism,
            pageSize,
          )
          .iterator
      )
      .map { range =>
        metrics.daml.services.index.getContractStateEventsChunkSize.update(
          range.endInclusive - range.startExclusive
        )
        range
      }
      // Dispatch database fetches in parallel
      .mapAsync(eventProcessingParallelism) { range =>
        dispatcher.executeSql(dbMetrics.getContractStateEvents) { implicit conn =>
          queryNonPruned.executeSql(
            storageBackend
              .contractStateEvents(range.startExclusive, range.endInclusive)(conn),
            startExclusive._1,
            pruned =>
              s"Contract state events request from ${range.startExclusive.toHexString} to ${range.endInclusive.toHexString} precedes pruned offset ${pruned.toHexString}",
          )
        }
      }
      .mapConcat(identity)
      .async
      .mapAsync(eventProcessingParallelism) { raw =>
        Timed.future(
          metrics.daml.index.decodeStateEvent,
          Future(ContractStateEventsReader.toContractStateEvent(raw, lfValueTranslation)),
        )
      }
      .map(event => (event.eventOffset, event.eventSequentialId) -> event)
      .mapMaterializedValue(_ => NotUsed)

    InstrumentedSource
      .bufferedSource(
        original = contractStateEventsSource,
        counter = metrics.daml.index.contractStateEventsBufferSize,
        size = outputStreamBufferSize,
      )
      .concat(endMarker)
  }

  private def nextPageRange[E](endEventSeqId: (Offset, Long))(
      a: EventsTable.Entry[E]
  ): EventsRange[(Offset, Long)] =
    EventsRange(startExclusive = (a.eventOffset, a.eventSequentialId), endInclusive = endEventSeqId)

  private def getAcsEventSeqIdRange(activeAt: Offset)(implicit
      loggingContext: LoggingContext
  ): Future[EventsRange[(Offset, Long)]] =
    dispatcher
      .executeSql(dbMetrics.getAcsEventSeqIdRange)(implicit connection =>
        queryNonPruned.executeSql(
          eventSeqIdReader.readEventSeqIdRange(activeAt)(connection),
          activeAt,
          pruned =>
            s"Active contracts request after ${activeAt.toHexString} precedes pruned offset ${pruned.toHexString}",
        )
      )
      .map { x =>
        EventsRange(
          startExclusive = (Offset.beforeBegin, 0),
          endInclusive = (activeAt, x.endInclusive),
        )
      }

  private def getEventSeqIdRange(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Future[EventsRange[(Offset, Long)]] =
    dispatcher
      .executeSql(dbMetrics.getEventSeqIdRange)(implicit connection =>
        queryNonPruned.executeSql(
          eventSeqIdReader.readEventSeqIdRange(EventsRange(startExclusive, endInclusive))(
            connection
          ),
          startExclusive,
          pruned =>
            s"Transactions request from ${startExclusive.toHexString} to ${endInclusive.toHexString} precedes pruned offset ${pruned.toHexString}",
        )
      )
      .map(x =>
        EventsRange(
          startExclusive = (startExclusive, x.startExclusive),
          endInclusive = (endInclusive, x.endInclusive),
        )
      )

  private def streamEvents[A: Ordering, E](
      verbose: Boolean,
      queryMetric: DatabaseMetrics,
      query: EventsRange[A] => Connection => Vector[EventsTable.Entry[Raw[E]]],
      getNextPageRange: EventsTable.Entry[E] => EventsRange[A],
  )(range: EventsRange[A])(implicit
      loggingContext: LoggingContext
  ): Source[EventsTable.Entry[E], NotUsed] =
    PaginatingAsyncStream.streamFrom(range, getNextPageRange) { range1 =>
      if (EventsRange.isEmpty(range1))
        Future.successful(Vector.empty)
      else {
        val rawEvents: Future[Vector[EventsTable.Entry[Raw[E]]]] =
          dispatcher.executeSql(queryMetric)(query(range1))
        rawEvents.flatMap(es =>
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

private[appendonlydao] object TransactionsReader {

  /** Splits a range of events into equally-sized sub-ranges.
    *
    * @param startExclusive The start exclusive of the range to be split
    * @param endInclusive The end inclusive of the range to be split
    * @param numberOfChunks The number of desired target sub-ranges
    * @param maxChunkSize Maximum sub-range size.
    * @return The ordered sequence of sub-ranges with non-overlapping bounds.
    */
  private[appendonlydao] def splitRange(
      startExclusive: Long,
      endInclusive: Long,
      numberOfChunks: Int,
      maxChunkSize: Int,
  ): Vector[EventsRange[Long]] = {
    require(
      maxChunkSize > 0,
      s"Maximum chunk size must be strictly positive, but was $maxChunkSize",
    )
    require(
      numberOfChunks > 0,
      s"You can only split a range in a strictly positive number of chunks ($numberOfChunks)",
    )

    val rangeSize = endInclusive - startExclusive
    require(
      rangeSize >= 0,
      s"Range size should be positive but got bounds ($startExclusive, $endInclusive]",
    )

    val minChunkSize = math.max(1, maxChunkSize / 10)

    if (rangeSize == 0L)
      Vector.empty
    else {
      val targetChunkSize = rangeSize / numberOfChunks.toLong

      if (targetChunkSize < minChunkSize.toLong) {
        val effectiveNumberOfChunks = rangeSize / minChunkSize.toLong

        if (effectiveNumberOfChunks <= 1) {
          Vector(EventsRange(startExclusive, endInclusive))
        } else {
          splitRangeUnsafe(startExclusive, rangeSize, effectiveNumberOfChunks.toInt)
        }
      } else {
        val effectiveNumberOfChunks =
          Math.max(
            numberOfChunks,
            Math.ceil(rangeSize.toDouble / maxChunkSize.toDouble).toInt,
          )
        splitRangeUnsafe(startExclusive, rangeSize, effectiveNumberOfChunks)
      }
    }
  }

  private def splitRangeUnsafe(
      startExclusive: Long,
      rangeSize: Long,
      numberOfChunks: Int,
  ): Vector[EventsRange[Long]] = {
    val minStep = rangeSize / numberOfChunks.toLong

    val remainder = rangeSize - minStep * numberOfChunks.toLong

    (0 until numberOfChunks)
      .foldLeft(
        (startExclusive, remainder, Vector.empty[EventsRange[Long]])
      ) {
        case ((lastStartExclusive, 0L, ranges), _) =>
          val endInclusiveChunk = lastStartExclusive + minStep
          (endInclusiveChunk, 0L, ranges :+ EventsRange(lastStartExclusive, endInclusiveChunk))

        case ((lastStartExclusive, remainder, ranges), _) =>
          val endInclusiveChunk = lastStartExclusive + minStep + 1L
          val rangesResult = ranges :+ EventsRange(lastStartExclusive, endInclusiveChunk)
          (endInclusiveChunk, remainder - 1L, rangesResult)
      }
      ._3
  }
}
