// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import akka.NotUsed
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import com.codahale.metrics.{Counter, Timer}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.{InstrumentedSource, Metrics, Timed}
import com.daml.platform.store.appendonlydao
import com.daml.platform.store.appendonlydao.LedgerDaoTransactionsReader
import com.daml.platform.store.appendonlydao.events.BufferedTransactionsReader.getTransactions
import com.daml.platform.store.appendonlydao.events.TransactionLogUpdatesConversions.{
  FilterResult,
  ToFlatTransaction,
  ToTransactionTree,
}
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.daml.platform.store.cache.{BufferSlice, EventsBuffer}
import com.daml.platform.store.interfaces.TransactionLogUpdate

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

private[events] class BufferedTransactionsReader(
    protected val delegate: LedgerDaoTransactionsReader,
    val transactionsBuffer: EventsBuffer[TransactionLogUpdate],
    lfValueTranslation: LfValueTranslation,
    metrics: Metrics,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoTransactionsReader {

  private val outputStreamBufferSize = 128

  private val flatTransactionsBufferMetrics =
    metrics.daml.services.index.BufferReader("flat_transactions")
  private val transactionTreesBufferMetrics =
    metrics.daml.services.index.BufferReader("transaction_trees")

  override def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FilterRelation,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] = {
    val (parties, partiesTemplates) = filter.partition(_._2.isEmpty)
    val wildcardParties = parties.keySet

    val templatesParties = invertMapping(partiesTemplates)

    getTransactions(transactionsBuffer)(startExclusive, endInclusive, filter, verbose)(
      filterEvents = ToFlatTransaction.filterT(_)(wildcardParties, templatesParties),
      toApiTx = ToFlatTransaction.toApiTx(_)(filter, verbose, lfValueTranslation),
      fetchTransactions = delegate.getFlatTransactions(_, _, _, _)(loggingContext),
      toApiTxTimer = flatTransactionsBufferMetrics.conversion,
      sourceTimer = flatTransactionsBufferMetrics.fetchTimer,
      resolvedFromBufferCounter = flatTransactionsBufferMetrics.fetchedBuffered,
      totalRetrievedCounter = flatTransactionsBufferMetrics.fetchedTotal,
      bufferSizeCounter = flatTransactionsBufferMetrics.bufferSize,
      outputStreamBufferSize = outputStreamBufferSize,
      inStreamBufferLength = flatTransactionsBufferMetrics.inStreamBufferLength,
    )
  }

  override def getTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] =
    getTransactions(transactionsBuffer)(startExclusive, endInclusive, requestingParties, verbose)(
      filterEvents = ToTransactionTree.filter(_)(requestingParties),
      toApiTx = ToTransactionTree.toApiTx(_)(requestingParties, verbose, lfValueTranslation),
      fetchTransactions = delegate.getTransactionTrees(_, _, _, _)(loggingContext),
      toApiTxTimer = transactionTreesBufferMetrics.conversion,
      sourceTimer = transactionTreesBufferMetrics.fetchTimer,
      resolvedFromBufferCounter = transactionTreesBufferMetrics.fetchedBuffered,
      totalRetrievedCounter = transactionTreesBufferMetrics.fetchedTotal,
      bufferSizeCounter = transactionTreesBufferMetrics.bufferSize,
      outputStreamBufferSize = outputStreamBufferSize,
      inStreamBufferLength = transactionTreesBufferMetrics.inStreamBufferLength,
    )

  override def lookupFlatTransactionById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    delegate.lookupFlatTransactionById(transactionId, requestingParties)

  override def lookupTransactionTreeById(
      transactionId: Ref.TransactionId,
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

  private def invertMapping(partiesTemplates: Map[Party, Set[appendonlydao.events.Identifier]]) =
    partiesTemplates
      .foldLeft(Map.empty[appendonlydao.events.Identifier, mutable.Builder[Party, Set[Party]]]) {
        case (acc, (k, vs)) =>
          vs.foldLeft(acc) { case (a, v) =>
            a + (v -> (a.getOrElse(v, Set.newBuilder) += k))
          }
      }
      .view
      .map { case (k, v) => k -> v.result() }
      .toMap
}

private[platform] object BufferedTransactionsReader {
  type FetchTransactions[FILTER, API_RESPONSE] =
    (Offset, Offset, FILTER, Boolean) => Source[(Offset, API_RESPONSE), NotUsed]

  def apply(
      delegate: LedgerDaoTransactionsReader,
      transactionsBuffer: EventsBuffer[TransactionLogUpdate],
      lfValueTranslation: LfValueTranslation,
      metrics: Metrics,
  )(implicit
      executionContext: ExecutionContext
  ): BufferedTransactionsReader =
    new BufferedTransactionsReader(
      delegate = delegate,
      transactionsBuffer = transactionsBuffer,
      metrics = metrics,
      lfValueTranslation = lfValueTranslation,
    )

  private[events] def getTransactions[FILTER, API_RESPONSE](
      transactionsBuffer: EventsBuffer[TransactionLogUpdate]
  )(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FILTER,
      verbose: Boolean,
  )(
      filterEvents: TransactionLogUpdatesConversions.Filter,
      toApiTx: FilterResult => Future[API_RESPONSE],
      fetchTransactions: FetchTransactions[FILTER, API_RESPONSE],
      sourceTimer: Timer,
      toApiTxTimer: Timer,
      resolvedFromBufferCounter: Counter,
      totalRetrievedCounter: Counter,
      outputStreamBufferSize: Int,
      bufferSizeCounter: Counter,
      inStreamBufferLength: Counter,
  )(implicit executionContext: ExecutionContext): Source[(Offset, API_RESPONSE), NotUsed] = {
    def getNextChunk(startExclusive: Offset): () => Source[(Offset, API_RESPONSE), NotUsed] = () =>
      getTransactions(transactionsBuffer)(
        startExclusive,
        endInclusive,
        filter,
        verbose,
      )(
        filterEvents,
        toApiTx,
        fetchTransactions,
        sourceTimer,
        toApiTxTimer,
        resolvedFromBufferCounter,
        totalRetrievedCounter,
        outputStreamBufferSize,
        bufferSizeCounter,
        inStreamBufferLength,
      )

    val transactionsSource = Timed.source(
      sourceTimer, {
        transactionsBuffer.slice(
          startExclusive,
          endInclusive,
          {
            case tx: TransactionLogUpdate.TransactionAccepted => filterEvents(tx)
            case _ => None
          },
        ) match {
          case BufferSlice.EmptyBuffer =>
            fetchTransactions(startExclusive, endInclusive, filter, verbose)

          case BufferSlice.EmptyPrefix(headOffset) =>
            fetchTransactions(startExclusive, headOffset, filter, verbose)

          case BufferSlice.EmptyResult => Source.empty

          case BufferSlice.Prefix(headOffset, tail, continue) =>
            fetchTransactions(startExclusive, headOffset, filter, verbose)
              .concat(
                sliceSource(
                  tail,
                  toApiTx,
                  toApiTxTimer,
                  inStreamBufferLength,
                  resolvedFromBufferCounter,
                )
              )
              .concatLazy {
                continue.map(from => Source.lazySource(getNextChunk(from))).getOrElse(Source.empty)
              }

          case BufferSlice.Inclusive(slice, continue) =>
            sliceSource(
              slice,
              toApiTx,
              toApiTxTimer,
              inStreamBufferLength,
              resolvedFromBufferCounter,
            ).concatLazy {
              continue.map(from => Source.lazySource(getNextChunk(from))).getOrElse(Source.empty)
            }
        }
      }.map(tx => {
        totalRetrievedCounter.inc()
        tx
      }),
    )

    InstrumentedSource.bufferedSource(
      original = transactionsSource,
      counter = bufferSizeCounter,
      size = outputStreamBufferSize,
    )
  }

  private def sliceSource[API_RESPONSE, FILTER](
      tail: Vector[(Offset, FilterResult)],
      toApiTx: FilterResult => Future[API_RESPONSE],
      toApiTxTimer: Timer,
      inStreamBufferLength: Counter,
      resolvedFromBufferCounter: Counter,
  ) =
    Source
      .fromIterator(() => tail.iterator)
      .async
      .buffered(128)(inStreamBufferLength)
      .mapAsync(4) { case (offset, payload) =>
        resolvedFromBufferCounter.inc()
        Timed.future(toApiTxTimer, toApiTx(payload).map(offset -> _)(ExecutionContext.parasitic))
      }

  private implicit class SourceWithBuffers[T, R](source: Source[T, NotUsed]) {
    def buffered(bufferLength: Int)(counter: com.codahale.metrics.Counter): Source[T, NotUsed] =
      source
        .map { in =>
          counter.inc()
          in
        }
        .buffer(bufferLength, OverflowStrategy.backpressure)
        .map { in =>
          counter.dec()
          in
        }
  }
}
