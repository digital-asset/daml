// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
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
import com.digitalasset.canton.{data, platform}
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
      (Option[Set[Party]], EventProjectionProperties),
      GetUpdateTreesResponse,
    ],
    bufferedFlatTransactionByIdReader: BufferedTransactionByIdReader[
      GetTransactionResponse,
    ],
    bufferedTransactionTreeByIdReader: BufferedTransactionByIdReader[
      GetTransactionTreeResponse,
    ],
    lfValueTranslation: LfValueTranslation,
    directEC: DirectExecutionContext,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoTransactionsReader {

  override def getFlatTransactions(
      startInclusive: Offset,
      endInclusive: Offset,
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] =
    bufferedFlatTransactionsReader
      .stream(
        startInclusive = startInclusive,
        endInclusive = endInclusive,
        persistenceFetchArgs = (filter, eventProjectionProperties),
        bufferFilter = ToFlatTransaction
          .filter(
            filter.templateWildcardParties,
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
            directEC,
          ),
      )

  override def getTransactionTrees(
      startInclusive: Offset,
      endInclusive: Offset,
      requestingParties: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdateTreesResponse), NotUsed] =
    bufferedTransactionTreesReader
      .stream(
        startInclusive = startInclusive,
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
            directEC,
          ),
      )

  override def lookupFlatTransactionById(
      updateId: data.UpdateId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] =
    Future.delegate(
      bufferedFlatTransactionByIdReader.fetch(updateId, requestingParties)
    )

  override def lookupFlatTransactionByOffset(
      offset: data.Offset,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] =
    delegate.lookupFlatTransactionByOffset(offset, requestingParties)

  override def lookupTransactionTreeById(
      updateId: data.UpdateId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]] =
    Future.delegate(
      bufferedTransactionTreeByIdReader.fetch(updateId, requestingParties)
    )

  override def lookupTransactionTreeByOffset(
      offset: Offset,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]] =
    delegate.lookupTransactionTreeByOffset(offset, requestingParties)

  override def getActiveContracts(
      activeAt: Option[Offset],
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
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): BufferedTransactionsReader = {
    val directEC = DirectExecutionContext(
      loggerFactory.getLogger(BufferedTransactionsReader.getClass)
    )

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
              startInclusive: Offset,
              endInclusive: Offset,
              filter: (TemplatePartiesFilter, EventProjectionProperties),
          )(implicit
              loggingContext: LoggingContextWithTrace
          ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
            val (partyTemplateFilter, eventProjectionProperties) = filter
            delegate
              .getFlatTransactions(
                startInclusive = startInclusive,
                endInclusive = endInclusive,
                filter = partyTemplateFilter,
                eventProjectionProperties = eventProjectionProperties,
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
        (Option[Set[Party]], EventProjectionProperties),
        GetUpdateTreesResponse,
      ](
        inMemoryFanoutBuffer = transactionsBuffer,
        fetchFromPersistence = new FetchFromPersistence[
          (Option[Set[Party]], EventProjectionProperties),
          GetUpdateTreesResponse,
        ] {
          override def apply(
              startInclusive: Offset,
              endInclusive: Offset,
              filter: (Option[Set[Party]], EventProjectionProperties),
          )(implicit
              loggingContext: LoggingContextWithTrace
          ): Source[(Offset, GetUpdateTreesResponse), NotUsed] = {
            val (requestingParties, eventProjectionProperties) = filter
            delegate
              .getTransactionTrees(
                startInclusive = startInclusive,
                endInclusive = endInclusive,
                requestingParties = requestingParties,
                eventProjectionProperties = eventProjectionProperties,
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
            updateId: String,
            requestingParties: Set[Party],
            loggingContext: LoggingContextWithTrace,
        ) =>
          delegate.lookupFlatTransactionById(
            platform.UpdateId.assertFromString(updateId),
            requestingParties,
          )(loggingContext),
        toApiResponse = (
            transactionAccepted: TransactionLogUpdate.TransactionAccepted,
            requestingParties: Set[Party],
            loggingContext: LoggingContextWithTrace,
        ) =>
          ToFlatTransaction.toGetFlatTransactionResponse(
            transactionAccepted,
            requestingParties,
            lfValueTranslation,
          )(loggingContext, directEC),
      )

    val bufferedTransactionTreeByIdReader =
      new BufferedTransactionByIdReader[GetTransactionTreeResponse](
        inMemoryFanoutBuffer = transactionsBuffer,
        fetchFromPersistence = (
            updateId: String,
            requestingParties: Set[Party],
            loggingContext: LoggingContextWithTrace,
        ) =>
          delegate.lookupTransactionTreeById(
            platform.UpdateId.assertFromString(updateId),
            requestingParties,
          )(loggingContext),
        toApiResponse = (
            transactionAccepted: TransactionLogUpdate.TransactionAccepted,
            requestingParties: Set[Party],
            loggingContext: LoggingContextWithTrace,
        ) =>
          ToTransactionTree.toGetTransactionResponse(
            transactionAccepted,
            requestingParties,
            lfValueTranslation,
          )(loggingContext, directEC),
      )

    new BufferedTransactionsReader(
      delegate = delegate,
      bufferedFlatTransactionsReader = flatTransactionsStreamReader,
      bufferedTransactionTreesReader = transactionTreesStreamReader,
      lfValueTranslation = lfValueTranslation,
      bufferedFlatTransactionByIdReader = bufferedFlatTransactionByIdReader,
      bufferedTransactionTreeByIdReader = bufferedTransactionTreeByIdReader,
      directEC = directEC,
    )
  }
}
