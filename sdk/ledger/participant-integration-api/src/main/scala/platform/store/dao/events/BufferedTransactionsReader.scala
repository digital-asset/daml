// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref.TransactionId
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.daml.platform.store.cache.{BufferSlice, EventsBuffer}
import com.daml.platform.store.dao.LedgerDaoTransactionsReader
import com.daml.platform.store.dao.events.BufferedTransactionsReader.{
  getTransactions,
  invertMapping,
}
import com.daml.platform.store.dao.events.TransactionLogUpdatesConversions.{
  ToApi,
  ToFlatTransaction,
  ToTransactionTree,
}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.{FilterRelation, Identifier, Party}

import scala.concurrent.{ExecutionContext, Future}

private[events] class BufferedTransactionsReader(
    protected val delegate: LedgerDaoTransactionsReader,
    val transactionsBuffer: EventsBuffer[TransactionLogUpdate],
    eventProcessingParallelism: Int,
    filterFlatTransactions: (
        Set[Party],
        Map[Identifier, Set[Party]],
    ) => TransactionLogUpdate.Transaction => Option[TransactionLogUpdate.Transaction],
    flatToApiTransactions: (
        FilterRelation,
        Boolean,
        LoggingContext,
    ) => ToApi[GetTransactionsResponse],
    filterTransactionTrees: Set[Party] => TransactionLogUpdate.Transaction => Option[
      TransactionLogUpdate.Transaction
    ],
    treesToApiTransactions: (
        Set[Party],
        Boolean,
        LoggingContext,
    ) => ToApi[GetTransactionTreesResponse],
    metrics: Metrics,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoTransactionsReader {

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
      eventProcessingParallelism = eventProcessingParallelism,
    )(
      filterEvents = filterFlatTransactions(wildcardParties, templatesParties),
      toApiTx = flatToApiTransactions(filter, verbose, loggingContext),
      fetchTransactions = delegate.getFlatTransactions(_, _, _, _)(loggingContext),
      bufferReaderMetrics = flatTransactionsBufferMetrics,
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
      eventProcessingParallelism = eventProcessingParallelism,
    )(
      filterEvents = filterTransactionTrees(requestingParties),
      toApiTx = treesToApiTransactions(requestingParties, verbose, loggingContext),
      fetchTransactions = delegate.getTransactionTrees(_, _, _, _)(loggingContext),
      bufferReaderMetrics = transactionTreesBufferMetrics,
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
}

private[platform] object BufferedTransactionsReader {
  type FetchTransactions[FILTER, API_RESPONSE] =
    (Offset, Offset, FILTER, Boolean) => Source[(Offset, API_RESPONSE), NotUsed]

  def apply(
      delegate: LedgerDaoTransactionsReader,
      transactionsBuffer: EventsBuffer[TransactionLogUpdate],
      eventProcessingParallelism: Int,
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
      eventProcessingParallelism = eventProcessingParallelism,
    )

  private[events] def getTransactions[FILTER, API_RESPONSE](
      transactionsBuffer: EventsBuffer[TransactionLogUpdate]
  )(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FILTER,
      verbose: Boolean,
      metrics: Metrics,
      eventProcessingParallelism: Int,
  )(
      filterEvents: TransactionLogUpdate.Transaction => Option[TransactionLogUpdate.Transaction],
      toApiTx: ToApi[API_RESPONSE],
      fetchTransactions: FetchTransactions[FILTER, API_RESPONSE],
      bufferReaderMetrics: metrics.daml.services.index.BufferedReader,
  )(implicit executionContext: ExecutionContext): Source[(Offset, API_RESPONSE), NotUsed] = {
    val sliceFilter: TransactionLogUpdate => Option[TransactionLogUpdate.Transaction] = {
      case tx: TransactionLogUpdate.Transaction => filterEvents(tx)
      case _ => None
    }

    def bufferSource(
        bufferSlice: Vector[(Offset, TransactionLogUpdate.Transaction)]
    ) =
      if (bufferSlice.isEmpty) Source.empty
      else
        Source(bufferSlice)
          .mapAsync(eventProcessingParallelism) { case (offset, payload) =>
            bufferReaderMetrics.fetchedBuffered.inc()
            Timed.future(
              bufferReaderMetrics.conversion,
              toApiTx(payload).map(offset -> _)(ExecutionContext.parasitic),
            )
          }

    val source = Source
      .unfoldAsync(startExclusive) {
        case scannedToInclusive if scannedToInclusive < endInclusive =>
          Future {
            transactionsBuffer.slice(scannedToInclusive, endInclusive, sliceFilter) match {
              case BufferSlice.Inclusive(slice) =>
                val sourceFromBuffer = bufferSource(slice)
                val nextChunkStartExclusive = slice.lastOption.map(_._1).getOrElse(endInclusive)
                Some(nextChunkStartExclusive -> sourceFromBuffer)

              case BufferSlice.LastBufferChunkSuffix(bufferedStartExclusive, slice) =>
                val sourceFromBuffer =
                  fetchTransactions(startExclusive, bufferedStartExclusive, filter, verbose)
                    .concat(bufferSource(slice))
                Some(endInclusive -> sourceFromBuffer)
            }
          }
        case _ => Future.successful(None)
      }
      .flatMapConcat(identity)

    Timed
      .source(bufferReaderMetrics.fetchTimer, source)
      .map { tx =>
        bufferReaderMetrics.fetchedTotal.inc()
        tx
      }
  }

  private[events] def invertMapping(
      partiesTemplates: Map[Party, Set[Identifier]]
  ): Map[Identifier, Set[Party]] =
    partiesTemplates
      .foldLeft(Map.empty[Identifier, Set[Party]]) {
        case (templatesToParties, (party, templates)) =>
          templates.foldLeft(templatesToParties) { case (aux, templateId) =>
            aux.updatedWith(templateId) {
              case None => Some(Set(party))
              case Some(partySet) => Some(partySet + party)
            }
          }
      }
}
