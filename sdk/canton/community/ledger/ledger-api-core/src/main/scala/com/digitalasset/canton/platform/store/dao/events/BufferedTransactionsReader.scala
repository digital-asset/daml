// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.daml.lf.data.Ref.TransactionId
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.dao.BufferedStreamsReader.FetchFromPersistence
import com.digitalasset.canton.platform.store.dao.events.TransactionLogUpdatesConversions.{
  ToFlatTransaction,
  ToTransactionTree,
}
import com.digitalasset.canton.platform.store.dao.{
  BufferedStreamsReader,
  BufferedTransactionByIdReader,
  EventProjectionProperties,
  LedgerDaoTransactionsReader,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.{Party, TemplatePartiesFilter}
import com.digitalasset.canton.tracing.Traced
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

private[events] class BufferedTransactionsReader(
    delegate: LedgerDaoTransactionsReader,
    bufferedFlatTransactionsReader: BufferedStreamsReader[
      (TemplatePartiesFilter, EventProjectionProperties),
      GetUpdatesResponse,
    ],
    bufferedTransactionTreesReader: BufferedStreamsReader[
      (Set[Party], EventProjectionProperties),
      GetUpdateTreesResponse,
    ],
    bufferedFlatTransactionByIdReader: BufferedTransactionByIdReader[
      GetTransactionResponse,
    ],
    bufferedTransactionTreeByIdReader: BufferedTransactionByIdReader[
      GetTransactionTreeResponse,
    ],
    lfValueTranslation: LfValueTranslation,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoTransactionsReader {

  override def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
    bufferedFlatTransactionsReader
      .stream(
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        persistenceFetchArgs = (filter, eventProjectionProperties),
        bufferFilter = ToFlatTransaction
          .filter(
            filter.wildcardParties,
            filter.relation,
            filter.allFilterParties,
          ),
        toApiResponse = ToFlatTransaction
          .toGetTransactionsResponse(
            filter,
            eventProjectionProperties,
            lfValueTranslation,
          )(
            loggingContext,
            executionContext,
          ),
      )
  }

  override def getTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdateTreesResponse), NotUsed] =
    bufferedTransactionTreesReader
      .stream(
        startExclusive = startExclusive,
        endInclusive = endInclusive,
        persistenceFetchArgs = (requestingParties, eventProjectionProperties),
        bufferFilter = ToTransactionTree.filter(requestingParties),
        toApiResponse = ToTransactionTree
          .toGetTransactionTreesResponse(
            requestingParties,
            eventProjectionProperties,
            lfValueTranslation,
          )(
            loggingContext,
            executionContext,
          ),
      )

  override def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] =
    bufferedFlatTransactionByIdReader.fetch(transactionId, requestingParties)

  override def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]] =
    bufferedTransactionTreeByIdReader.fetch(transactionId, requestingParties)

  override def getActiveContracts(
      activeAt: Offset,
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] =
    delegate.getActiveContracts(activeAt, filter, eventProjectionProperties)
}

private[platform] object BufferedTransactionsReader {
  def apply(
      delegate: LedgerDaoTransactionsReader,
      transactionsBuffer: InMemoryFanoutBuffer,
      eventProcessingParallelism: Int,
      lfValueTranslation: LfValueTranslation,
      metrics: Metrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): BufferedTransactionsReader = {
    val flatTransactionsStreamReader =
      new BufferedStreamsReader[
        (TemplatePartiesFilter, EventProjectionProperties),
        GetUpdatesResponse,
      ](
        inMemoryFanoutBuffer = transactionsBuffer,
        fetchFromPersistence = new FetchFromPersistence[
          (TemplatePartiesFilter, EventProjectionProperties),
          GetUpdatesResponse,
        ] {
          override def apply(
              startExclusive: Offset,
              endInclusive: Offset,
              filter: (TemplatePartiesFilter, EventProjectionProperties),
          )(implicit
              loggingContext: LoggingContextWithTrace
          ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
            val (partyTemplateFilter, eventProjectionProperties) = filter
            delegate.getFlatTransactions(
              startExclusive,
              endInclusive,
              partyTemplateFilter,
              eventProjectionProperties,
            )
          }
        },
        bufferedStreamEventsProcessingParallelism = eventProcessingParallelism,
        metrics = metrics,
        streamName = "transactions",
        loggerFactory,
      )

    val transactionTreesStreamReader =
      new BufferedStreamsReader[
        (Set[Party], EventProjectionProperties),
        GetUpdateTreesResponse,
      ](
        inMemoryFanoutBuffer = transactionsBuffer,
        fetchFromPersistence = new FetchFromPersistence[
          (Set[Party], EventProjectionProperties),
          GetUpdateTreesResponse,
        ] {
          override def apply(
              startExclusive: Offset,
              endInclusive: Offset,
              filter: (Set[Party], EventProjectionProperties),
          )(implicit
              loggingContext: LoggingContextWithTrace
          ): Source[(Offset, GetUpdateTreesResponse), NotUsed] = {
            val (requestingParties, eventProjectionProperties) = filter
            delegate.getTransactionTrees(
              startExclusive,
              endInclusive,
              requestingParties,
              eventProjectionProperties,
            )
          }
        },
        bufferedStreamEventsProcessingParallelism = eventProcessingParallelism,
        metrics = metrics,
        streamName = "transaction_trees",
        loggerFactory,
      )

    val bufferedFlatTransactionByIdReader =
      new BufferedTransactionByIdReader[GetTransactionResponse](
        inMemoryFanoutBuffer = transactionsBuffer,
        fetchFromPersistence = (
            transactionId: String,
            requestingParties: Set[Party],
            loggingContext: LoggingContextWithTrace,
        ) =>
          delegate.lookupFlatTransactionById(
            platform.TransactionId.assertFromString(transactionId),
            requestingParties,
          )(loggingContext),
        toApiResponse = (
            transactionAccepted: Traced[TransactionLogUpdate.TransactionAccepted],
            requestingParties: Set[Party],
            loggingContext: LoggingContextWithTrace,
        ) =>
          ToFlatTransaction.toGetFlatTransactionResponse(
            transactionAccepted,
            requestingParties,
            lfValueTranslation,
          )(loggingContext, executionContext),
      )

    val bufferedTransactionTreeByIdReader =
      new BufferedTransactionByIdReader[GetTransactionTreeResponse](
        inMemoryFanoutBuffer = transactionsBuffer,
        fetchFromPersistence = (
            transactionId: String,
            requestingParties: Set[Party],
            loggingContext: LoggingContextWithTrace,
        ) =>
          delegate.lookupTransactionTreeById(
            platform.TransactionId.assertFromString(transactionId),
            requestingParties,
          )(loggingContext),
        toApiResponse = (
            transactionAccepted: Traced[TransactionLogUpdate.TransactionAccepted],
            requestingParties: Set[Party],
            loggingContext: LoggingContextWithTrace,
        ) =>
          ToTransactionTree.toGetTransactionResponse(
            transactionAccepted,
            requestingParties,
            lfValueTranslation,
          )(loggingContext, executionContext),
      )

    new BufferedTransactionsReader(
      delegate = delegate,
      bufferedFlatTransactionsReader = flatTransactionsStreamReader,
      bufferedTransactionTreesReader = transactionTreesStreamReader,
      lfValueTranslation = lfValueTranslation,
      bufferedFlatTransactionByIdReader = bufferedFlatTransactionByIdReader,
      bufferedTransactionTreeByIdReader = bufferedTransactionTreeByIdReader,
    )
  }
}
