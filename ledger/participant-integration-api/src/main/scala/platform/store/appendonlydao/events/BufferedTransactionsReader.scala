// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.{Counter, Timer}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.transaction.{TransactionTree, Transaction => FlatTransaction}
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.participant.state.v1.{Offset, TransactionId}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{InstrumentedSource, Metrics, Timed}
import com.daml.platform.store.appendonlydao.events.BufferedTransactionsReader.getTransactions
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.daml.platform.store.cache.{BufferSlice, EventsBuffer}
import com.daml.platform.store.dao.LedgerDaoTransactionsReader
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.{Transaction => TxUpdate}

import scala.concurrent.{ExecutionContext, Future}

private[events] class BufferedTransactionsReader(
    protected val delegate: LedgerDaoTransactionsReader,
    val transactionsBuffer: EventsBuffer[Offset, TransactionLogUpdate],
    toFlatTransaction: (TxUpdate, FilterRelation, Boolean) => Future[Option[FlatTransaction]],
    toTransactionTree: (TxUpdate, Set[Party], Boolean) => Future[Option[TransactionTree]],
    metrics: Metrics,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoTransactionsReader {

  private val OutputStreamBufferSize = 128

  override def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FilterRelation,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] =
    getTransactions(transactionsBuffer)(startExclusive, endInclusive, filter, verbose)(
      toApiTx = toFlatTransaction,
      apiResponseCtor = GetTransactionsResponse(_),
      fetchTransactions = delegate.getFlatTransactions(_, _, _, _)(loggingContext),
      sourceTimer = metrics.daml.services.index.streamsBuffer.getFlatTransactions,
      resolvedFromBufferCounter =
        metrics.daml.services.index.streamsBuffer.flatTransactionsBuffered,
      totalRetrievedCounter = metrics.daml.services.index.streamsBuffer.flatTransactionsTotal,
      bufferSizeCounter =
        // TODO in-memory fan-out: Specialize the metric per consumer
        metrics.daml.services.index.flatTransactionsBufferSize,
      outputStreamBufferSize = OutputStreamBufferSize,
    )

  override def getTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] =
    getTransactions(transactionsBuffer)(startExclusive, endInclusive, requestingParties, verbose)(
      toApiTx = toTransactionTree,
      apiResponseCtor = GetTransactionTreesResponse(_),
      fetchTransactions = delegate.getTransactionTrees(_, _, _, _)(loggingContext),
      sourceTimer = metrics.daml.services.index.streamsBuffer.getTransactionTrees,
      resolvedFromBufferCounter =
        metrics.daml.services.index.streamsBuffer.transactionTreesBuffered,
      totalRetrievedCounter = metrics.daml.services.index.streamsBuffer.transactionTreesTotal,
      bufferSizeCounter =
        // TODO in-memory fan-out: Specialize the metric per consumer
        metrics.daml.services.index.transactionTreesBufferSize,
      outputStreamBufferSize = OutputStreamBufferSize,
    )

  override def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    delegate.lookupFlatTransactionById(transactionId, requestingParties)

  override def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    delegate.lookupTransactionTreeById(transactionId, requestingParties)

  override def getActiveContracts(activeAt: Offset, filter: FilterRelation, verbose: Boolean)(
      implicit loggingContext: LoggingContext
  ): Source[GetActiveContractsResponse, NotUsed] =
    delegate.getActiveContracts(activeAt, filter, verbose)

  override def getContractStateEvents(startExclusive: (Offset, Long), endInclusive: (Offset, Long))(
      implicit loggingContext: LoggingContext
  ): Source[((Offset, Long), ContractStateEvent), NotUsed] =
    throw new UnsupportedOperationException(
      s"getContractStateEvents is not supported on ${getClass.getSimpleName}"
    )

  override def getTransactionLogUpdates(
      startExclusive: (Offset, EventSequentialId),
      endInclusive: (Offset, EventSequentialId),
  )(implicit
      loggingContext: LoggingContext
  ): Source[((Offset, EventSequentialId), TransactionLogUpdate), NotUsed] =
    throw new UnsupportedOperationException(
      s"getTransactionLogUpdates is not supported on ${getClass.getSimpleName}"
    )
}

private[platform] object BufferedTransactionsReader {
  type FetchTransactions[FILTER, API_RESPONSE] =
    (Offset, Offset, FILTER, Boolean) => Source[(Offset, API_RESPONSE), NotUsed]
  private val logger = ContextualizedLogger.get(getClass)

  def apply(
      delegate: LedgerDaoTransactionsReader,
      transactionsBuffer: EventsBuffer[Offset, TransactionLogUpdate],
      lfValueTranslation: LfValueTranslation,
      metrics: Metrics,
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): BufferedTransactionsReader =
    new BufferedTransactionsReader(
      delegate = delegate,
      transactionsBuffer = transactionsBuffer,
      toFlatTransaction =
        TransactionLogUpdatesConversions.ToFlatTransaction(_, _, _, lfValueTranslation),
      toTransactionTree =
        TransactionLogUpdatesConversions.ToTransactionTree(_, _, _, lfValueTranslation),
      metrics = metrics,
    )

  private[events] def getTransactions[FILTER, API_TX, API_RESPONSE](
      transactionsBuffer: EventsBuffer[Offset, TransactionLogUpdate]
  )(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FILTER,
      verbose: Boolean,
  )(
      toApiTx: (TxUpdate, FILTER, Boolean) => Future[Option[API_TX]],
      apiResponseCtor: Seq[API_TX] => API_RESPONSE,
      fetchTransactions: FetchTransactions[FILTER, API_RESPONSE],
      sourceTimer: Timer,
      resolvedFromBufferCounter: Counter,
      totalRetrievedCounter: Counter,
      outputStreamBufferSize: Int,
      bufferSizeCounter: Counter,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Source[(Offset, API_RESPONSE), NotUsed] = {

    def filterBuffered(
        slice: Vector[(Offset, TransactionLogUpdate)]
    ): Future[Iterator[(Offset, API_RESPONSE)]] =
      Future
        .traverse(
          slice.iterator
            .collect { case (offset, tx: TxUpdate) =>
              toApiTx(tx, filter, verbose).map(offset -> _)
            }
        )(identity)
        .map(_.collect { case (offset, Some(tx)) =>
          resolvedFromBufferCounter.inc()
          offset -> apiResponseCtor(Seq(tx))
        })

    val timedTransactionsSource = Timed.source(
      sourceTimer,
      buildTransactionsSource(
        transactionsBuffer,
        startExclusive,
        endInclusive,
        filter,
        verbose,
        fetchTransactions,
        filterBuffered,
      ),
    )

    InstrumentedSource.bufferedSource(
      original = timedTransactionsSource.alsoTo(Sink.foreach(_ => totalRetrievedCounter.inc())),
      counter = bufferSizeCounter,
      size = outputStreamBufferSize,
    )
  }

  private def buildTransactionsSource[API_RESPONSE, API_TX, FILTER](
      transactionsBuffer: EventsBuffer[Offset, TransactionLogUpdate],
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FILTER,
      verbose: Boolean,
      fetchTransactions: FetchTransactions[FILTER, API_RESPONSE],
      filterBuffered: Vector[(Offset, TransactionLogUpdate)] => Future[
        Iterator[(Offset, API_RESPONSE)]
      ],
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Source[(Offset, API_RESPONSE), NotUsed] =
    // TODO Scala 2.13.5: Remove the @unchecked once migrated to Scala 2.13.5 where this false positive exhaustivity check for Vectors is fixed
    (transactionsBuffer.slice(startExclusive, endInclusive): @unchecked) match {
      case BufferSlice.Empty =>
        fetchTransactions(startExclusive, endInclusive, filter, verbose)

      case BufferSlice.Prefix(buffered @ (bufferedStartInclusive, _) +: _) =>
        val fetchEndInclusive = bufferedStartInclusive.predecessor

        val bufferedSource = Source.futureSource(
          filterBuffered(buffered).map(it => Source.fromIterator(() => it))
        )

        val source =
          if (fetchEndInclusive > startExclusive)
            fetchTransactions(startExclusive, fetchEndInclusive, filter, verbose)
              .concat(bufferedSource)
          else bufferedSource

        source.mapMaterializedValue(_ => NotUsed)

      case BufferSlice.Inclusive(slice) =>
        Source
          .futureSource(filterBuffered(slice).map(it => Source.fromIterator(() => it)))
          .mapMaterializedValue(_ => NotUsed)

      case BufferSlice.Prefix(Vector()) =>
        val className = classOf[BufferSlice.Prefix[_]].getSimpleName
        logger.warn(s"Unexpected empty vector in $className")

        fetchTransactions(startExclusive, endInclusive, filter, verbose)
    }
}
