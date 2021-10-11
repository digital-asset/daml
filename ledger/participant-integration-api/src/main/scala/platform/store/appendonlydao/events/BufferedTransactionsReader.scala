// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.{Counter, Timer}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.transaction.{TransactionTree, Transaction => FlatTransaction}
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
import com.daml.platform.store.appendonlydao.events
import com.daml.platform.store.appendonlydao.events.BufferedTransactionsReader.getTransactions
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.daml.platform.store.cache.{BufferSlice, EventsBuffer}
import com.daml.platform.store.dao.LedgerDaoTransactionsReader
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.{Transaction => TxUpdate}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

private[events] class BufferedTransactionsReader(
    protected val delegate: LedgerDaoTransactionsReader,
    val transactionsBuffer: EventsBuffer[Offset, TransactionLogUpdate],
    toFlatTransaction: (
        TxUpdate,
        FilterRelation,
        Set[String],
        Map[events.Identifier, Set[String]],
        Boolean,
    ) => Future[Option[FlatTransaction]],
    toTransactionTree: (TxUpdate, Set[String], Boolean) => Future[Option[TransactionTree]],
    metrics: Metrics,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoTransactionsReader {

  private val outputStreamBufferSize = 128

  override def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FilterRelation,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] = {
    val (parties, partiesTemplates) = filter.partition(_._2.isEmpty)
    val wildcardParties = parties.keySet.map(_.toString)

    val templatesParties = invertMapping(partiesTemplates)

    getTransactions(transactionsBuffer)(startExclusive, endInclusive, filter, verbose)(
      toApiTx = toFlatTransaction(_, _, wildcardParties, templatesParties, _),
      apiResponseCtor = GetTransactionsResponse(_),
      fetchTransactions = delegate.getFlatTransactions(_, _, _, _)(loggingContext),
      toApiTxTimer = metrics.daml.services.index.streamsBuffer.toFlatTransactions,
      sourceTimer = metrics.daml.services.index.streamsBuffer.getFlatTransactions,
      resolvedFromBufferCounter =
        metrics.daml.services.index.streamsBuffer.flatTransactionsBuffered,
      totalRetrievedCounter = metrics.daml.services.index.streamsBuffer.flatTransactionsTotal,
      bufferSizeCounter = metrics.daml.services.index.streamsBuffer.flatTransactionsBufferSize,
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
    getTransactions(transactionsBuffer)(startExclusive, endInclusive, requestingParties, verbose)(
      toApiTx = (tx: TxUpdate, requestingParties: Set[Party], verbose) =>
        toTransactionTree(tx, requestingParties.map(_.toString), verbose),
      apiResponseCtor = GetTransactionTreesResponse(_),
      fetchTransactions = delegate.getTransactionTrees(_, _, _, _)(loggingContext),
      toApiTxTimer = metrics.daml.services.index.streamsBuffer.toTransactionTrees,
      sourceTimer = metrics.daml.services.index.streamsBuffer.getTransactionTrees,
      resolvedFromBufferCounter =
        metrics.daml.services.index.streamsBuffer.transactionTreesBuffered,
      totalRetrievedCounter = metrics.daml.services.index.streamsBuffer.transactionTreesTotal,
      bufferSizeCounter =
        // TODO in-memory fan-out: Specialize the metric per consumer
        metrics.daml.services.index.streamsBuffer.transactionTreesBufferSize,
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
      .foldLeft(Map.empty[appendonlydao.events.Identifier, mutable.Builder[String, Set[String]]]) {
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
        TransactionLogUpdatesConversions.ToFlatTransaction(_, _, _, _, _, lfValueTranslation),
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
      toApiTxTimer: Timer,
      resolvedFromBufferCounter: Counter,
      totalRetrievedCounter: Counter,
      outputStreamBufferSize: Int,
      bufferSizeCounter: Counter,
  )(implicit executionContext: ExecutionContext): Source[(Offset, API_RESPONSE), NotUsed] = {
    def bufferedSource(
        slice: Vector[(Offset, TransactionLogUpdate)]
    ): Source[(Offset, API_RESPONSE), NotUsed] =
      Source
        .fromIterator(() => slice.iterator)
        // Using collect + mapAsync as an alternative to the non-existent collectAsync
        .collect { case (offset, tx: TxUpdate) =>
          Timed.future(toApiTxTimer, toApiTx(tx, filter, verbose).map(offset -> _))
        }
        // Note that it is safe to use high parallelism for mapAsync as long
        // as the Futures executed within are running on a bounded thread pool
        .mapAsync(32)(identity)
        .async
        .collect { case (offset, Some(tx)) =>
          resolvedFromBufferCounter.inc()
          offset -> apiResponseCtor(Seq(tx))
        }

    val transactionsSource = Timed.source(
      sourceTimer, {
        // TODO Scala 2.13.5: Remove the @unchecked once migrated to Scala 2.13.5 where this false positive exhaustivity check for Vectors is fixed
        (transactionsBuffer.slice(startExclusive, endInclusive): @unchecked) match {
          case BufferSlice.Empty =>
            fetchTransactions(startExclusive, endInclusive, filter, verbose)

          case BufferSlice.Prefix(slice) if slice.size <= 1 =>
            fetchTransactions(startExclusive, endInclusive, filter, verbose)

          case BufferSlice.Prefix((firstOffset: Offset, _) +: tl) =>
            fetchTransactions(startExclusive, firstOffset, filter, verbose)
              .concat(bufferedSource(tl))
              .mapMaterializedValue(_ => NotUsed)

          case BufferSlice.Inclusive(slice) =>
            bufferedSource(slice).mapMaterializedValue(_ => NotUsed)
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
}
