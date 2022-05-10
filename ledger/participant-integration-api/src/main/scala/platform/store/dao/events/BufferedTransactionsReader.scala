// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

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
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.daml.platform.store.cache.{BufferSlice, EventsBuffer}
import com.daml.platform.store.dao.LedgerDaoTransactionsReader
import com.daml.platform.store.dao.events.BufferedTransactionsReader.getTransactions
import com.daml.platform.store.dao.events.TransactionLogUpdatesConversions.{
  Filter,
  ToApi,
  ToFlatTransaction,
  ToTransactionTree,
}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.{FilterRelation, Identifier, Party}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

private[events] class BufferedTransactionsReader(
    protected val delegate: LedgerDaoTransactionsReader,
    val transactionsBuffer: EventsBuffer[TransactionLogUpdate],
    filterFlatTransactions: (Set[Party], Map[Identifier, Set[Party]]) => Filter,
    flatToApiTransactions: (
        FilterRelation,
        Boolean,
        LoggingContext,
    ) => ToApi[GetTransactionsResponse],
    filterTransactionTrees: Set[Party] => Filter,
    treesToApiTransactions: (
        Set[Party],
        Boolean,
        LoggingContext,
    ) => ToApi[GetTransactionTreesResponse],
    metrics: Metrics,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoTransactionsReader {

  private val outputStreamBufferSize = 128

  private val flatTransactionsBufferMetrics =
    metrics.daml.services.index.BufferedReader("flat_transactions")
  private val transactionTreesBufferMetrics =
    metrics.daml.services.index.BufferedReader("transaction_trees")

  override def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FilterRelation,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] = {
    val (parties, partiesTemplates) = filter.partition(_._2.isEmpty)
    val wildcardParties = parties.keySet

    val templatesParties = invertMapping(partiesTemplates)

    getTransactions(transactionsBuffer)(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      filter = filter,
      verbose = verbose,
      metrics = metrics,
    )(
      filterEvents = filterFlatTransactions(wildcardParties, templatesParties),
      toApiTx = flatToApiTransactions(filter, verbose, loggingContext),
      fetchTransactions = delegate.getFlatTransactions(_, _, _, _)(loggingContext),
      bufferReaderMetrics = flatTransactionsBufferMetrics,
      outputStreamBufferSize = outputStreamBufferSize,
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
    getTransactions(transactionsBuffer)(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      filter = requestingParties,
      verbose = verbose,
      metrics = metrics,
    )(
      filterEvents = filterTransactionTrees(requestingParties),
      toApiTx = treesToApiTransactions(requestingParties, verbose, loggingContext),
      fetchTransactions = delegate.getTransactionTrees(_, _, _, _)(loggingContext),
      bufferReaderMetrics = transactionTreesBufferMetrics,
      outputStreamBufferSize = outputStreamBufferSize,
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
  ): Source[((Offset, Long), Vector[ContractStateEvent]), NotUsed] =
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

  private def invertMapping(partiesTemplates: Map[Party, Set[Identifier]]) =
    partiesTemplates
      .foldLeft(Map.empty[Identifier, mutable.Builder[Party, Set[Party]]]) { case (acc, (k, vs)) =>
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
      filterFlatTransactions = ToFlatTransaction.filter,
      flatToApiTransactions =
        ToFlatTransaction.toApiTransaction(_, _, lfValueTranslation)(_, executionContext),
      filterTransactionTrees = ToTransactionTree.filter,
      treesToApiTransactions =
        ToTransactionTree.toApiTransaction(_, _, lfValueTranslation)(_, executionContext),
    )

  private[events] def getTransactions[FILTER, API_RESPONSE](
      transactionsBuffer: EventsBuffer[TransactionLogUpdate]
  )(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FILTER,
      verbose: Boolean,
      metrics: Metrics,
  )(
      filterEvents: Filter,
      toApiTx: ToApi[API_RESPONSE],
      fetchTransactions: FetchTransactions[FILTER, API_RESPONSE],
      bufferReaderMetrics: metrics.daml.services.index.BufferedReader,
      outputStreamBufferSize: Int,
  )(implicit executionContext: ExecutionContext): Source[(Offset, API_RESPONSE), NotUsed] = {
    def getNextChunk(startExclusive: Offset): () => Source[(Offset, API_RESPONSE), NotUsed] = () =>
      getTransactions(transactionsBuffer)(
        startExclusive,
        endInclusive,
        filter,
        verbose,
        metrics,
      )(
        filterEvents,
        toApiTx,
        fetchTransactions,
        bufferReaderMetrics,
        outputStreamBufferSize,
      )

    Timed
      .source(
        bufferReaderMetrics.fetchTimer, {
          transactionsBuffer.slice(
            startExclusive,
            endInclusive,
            {
              case tx: TransactionLogUpdate.Transaction => filterEvents(tx)
              case _ => None
            },
          ) match {
            case BufferSlice.Suffix(headOffset, tail, continue) =>
              fetchTransactions(startExclusive, headOffset, filter, verbose)
                .concat(
                  sliceSource(
                    tail,
                    toApiTx,
                    bufferReaderMetrics.conversion,
                    bufferReaderMetrics.inStreamBufferLength,
                    bufferReaderMetrics.fetchedBuffered,
                  )
                )
                .concatLazy {
                  continue
                    .map(from => Source.lazySource(getNextChunk(from)))
                    .getOrElse(Source.empty)
                }

            case BufferSlice.Inclusive(slice, continue) =>
              sliceSource(
                slice,
                toApiTx,
                bufferReaderMetrics.conversion,
                bufferReaderMetrics.inStreamBufferLength,
                bufferReaderMetrics.fetchedBuffered,
              ).concatLazy {
                continue.map(from => Source.lazySource(getNextChunk(from))).getOrElse(Source.empty)
              }
          }
        }.map(tx => {
          bufferReaderMetrics.fetchedTotal.inc()
          tx
        }),
      )
      .buffered(outputStreamBufferSize)(bufferReaderMetrics.bufferSize)
  }

  private def sliceSource[API_RESPONSE, FILTER](
      bufferSlice: Vector[(Offset, TransactionLogUpdate.Transaction)],
      toApiTx: TransactionLogUpdate.Transaction => Future[API_RESPONSE],
      toApiTxTimer: Timer,
      inStreamBufferLength: Counter,
      resolvedFromBuffer: Counter,
  ) =
    if (bufferSlice.isEmpty) Source.empty
    else {
      Source
        .fromIterator(() => bufferSlice.iterator)
        .async
        .buffered(128)(inStreamBufferLength)
        .mapAsync(1) { case (offset, payload) =>
          resolvedFromBuffer.inc()
          Timed.future(toApiTxTimer, toApiTx(payload).map(offset -> _)(ExecutionContext.parasitic))
        }
    }

  // TODO LLP: Consider merging with InstrumentedSource.bufferedSource
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
