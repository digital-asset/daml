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
  BufferedTransactionPointwiseReader,
  EventProjectionProperties,
  LedgerDaoUpdateReader,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.{
  InternalTransactionFormat,
  InternalUpdateFormat,
  Party,
  TemplatePartiesFilter,
}
import com.digitalasset.canton.{data, platform}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

private[events] class BufferedUpdateReader(
    delegate: LedgerDaoUpdateReader,
    bufferedUpdatesReader: BufferedStreamsReader[InternalUpdateFormat, GetUpdatesResponse],
    bufferedTransactionTreesReader: BufferedStreamsReader[
      (Option[Set[Party]], EventProjectionProperties),
      GetUpdateTreesResponse,
    ],
    bufferedTransactionByIdReader: BufferedTransactionPointwiseReader[
      (String, InternalTransactionFormat),
      GetTransactionResponse,
    ],
    bufferedTransactionTreeByIdReader: BufferedTransactionPointwiseReader[
      (String, Set[Party]),
      GetTransactionTreeResponse,
    ],
    bufferedTransactionByOffsetReader: BufferedTransactionPointwiseReader[
      (Offset, InternalTransactionFormat),
      GetTransactionResponse,
    ],
    bufferedTransactionTreeByOffsetReader: BufferedTransactionPointwiseReader[
      (Offset, Set[Party]),
      GetTransactionTreeResponse,
    ],
    lfValueTranslation: LfValueTranslation,
    directEC: DirectExecutionContext,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoUpdateReader {

  override def getUpdates(
      startInclusive: Offset,
      endInclusive: Offset,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] =
    bufferedUpdatesReader
      .stream(
        startInclusive = startInclusive,
        endInclusive = endInclusive,
        persistenceFetchArgs = internalUpdateFormat,
        bufferFilter = ToFlatTransaction
          .filter(internalUpdateFormat),
        toApiResponse = ToFlatTransaction
          .toGetUpdatesResponse(internalUpdateFormat, lfValueTranslation)(
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
        bufferFilter = ToTransactionTree
          .filter(requestingParties),
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

  override def lookupTransactionById(
      updateId: data.UpdateId,
      internalTransactionFormat: InternalTransactionFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] =
    Future.delegate(
      bufferedTransactionByIdReader.fetch(updateId -> internalTransactionFormat)
    )

  override def lookupTransactionByOffset(
      offset: data.Offset,
      internalTransactionFormat: InternalTransactionFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] =
    Future.delegate(
      bufferedTransactionByOffsetReader.fetch(offset -> internalTransactionFormat)
    )

  override def lookupTransactionTreeById(
      updateId: data.UpdateId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]] =
    Future.delegate(
      bufferedTransactionTreeByIdReader.fetch(updateId -> requestingParties)
    )

  override def lookupTransactionTreeByOffset(
      offset: Offset,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]] =
    Future.delegate(
      bufferedTransactionTreeByOffsetReader.fetch(offset -> requestingParties)
    )

  override def getActiveContracts(
      activeAt: Option[Offset],
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] =
    delegate.getActiveContracts(activeAt, filter, eventProjectionProperties)
}

private[platform] object BufferedUpdateReader {
  def apply(
      delegate: LedgerDaoUpdateReader,
      transactionsBuffer: InMemoryFanoutBuffer,
      eventProcessingParallelism: Int,
      lfValueTranslation: LfValueTranslation,
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): BufferedUpdateReader = {
    val directEC = DirectExecutionContext(
      loggerFactory.getLogger(BufferedUpdateReader.getClass)
    )

    val UpdatesStreamReader =
      new BufferedStreamsReader[InternalUpdateFormat, GetUpdatesResponse](
        inMemoryFanoutBuffer = transactionsBuffer,
        fetchFromPersistence = new FetchFromPersistence[InternalUpdateFormat, GetUpdatesResponse] {
          override def apply(
              startInclusive: Offset,
              endInclusive: Offset,
              filter: InternalUpdateFormat,
          )(implicit
              loggingContext: LoggingContextWithTrace
          ): Source[(Offset, GetUpdatesResponse), NotUsed] =
            delegate
              .getUpdates(
                startInclusive = startInclusive,
                endInclusive = endInclusive,
                internalUpdateFormat = filter,
              )
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

    val bufferedTransactionByIdReader =
      new BufferedTransactionPointwiseReader[
        (String, InternalTransactionFormat),
        GetTransactionResponse,
      ](
        fetchFromPersistence = {
          case (
                (updateId, internalTransactionFormat),
                loggingContext: LoggingContextWithTrace,
              ) =>
            delegate.lookupTransactionById(
              updateId = platform.UpdateId.assertFromString(updateId),
              internalTransactionFormat = internalTransactionFormat,
            )(loggingContext)
        },
        fetchFromBuffer = queryParam => transactionsBuffer.lookup(queryParam._1),
        toApiResponse = {
          case (
                transactionAccepted: TransactionLogUpdate.TransactionAccepted,
                (_updateId, internalTransactionFormat),
                loggingContext: LoggingContextWithTrace,
              ) =>
            ToFlatTransaction.toGetFlatTransactionResponse(
              transactionLogUpdate = transactionAccepted,
              internalTransactionFormat = internalTransactionFormat,
              lfValueTranslation = lfValueTranslation,
            )(loggingContext, directEC)
        },
      )

    val bufferedTransactionTreeByIdReader =
      new BufferedTransactionPointwiseReader[(String, Set[Party]), GetTransactionTreeResponse](
        fetchFromPersistence = {
          case (
                (updateId, parties),
                loggingContext: LoggingContextWithTrace,
              ) =>
            delegate.lookupTransactionTreeById(
              updateId = platform.UpdateId.assertFromString(updateId),
              requestingParties = parties,
            )(loggingContext)
        },
        fetchFromBuffer = { case (updateId, _) => transactionsBuffer.lookup(updateId) },
        toApiResponse = {
          case (
                transactionAccepted: TransactionLogUpdate.TransactionAccepted,
                (_updateId, parties),
                loggingContext: LoggingContextWithTrace,
              ) =>
            ToTransactionTree.toGetTransactionResponse(
              transactionLogUpdate = transactionAccepted,
              requestingParties = parties,
              lfValueTranslation = lfValueTranslation,
            )(loggingContext, directEC)
        },
      )

    val bufferedTransactionByOffsetReader =
      new BufferedTransactionPointwiseReader[
        (Offset, InternalTransactionFormat),
        GetTransactionResponse,
      ](
        fetchFromPersistence = {
          case (
                (offset, internalTransactionFormat),
                loggingContext: LoggingContextWithTrace,
              ) =>
            delegate.lookupTransactionByOffset(
              offset = offset,
              internalTransactionFormat = internalTransactionFormat,
            )(loggingContext)
        },
        fetchFromBuffer = queryParam => transactionsBuffer.lookup(queryParam._1),
        toApiResponse = (
            transactionAccepted: TransactionLogUpdate.TransactionAccepted,
            queryParam: (Offset, InternalTransactionFormat),
            loggingContext: LoggingContextWithTrace,
        ) =>
          ToFlatTransaction.toGetFlatTransactionResponse(
            transactionAccepted,
            queryParam._2,
            lfValueTranslation,
          )(loggingContext, directEC),
      )

    val bufferedTransactionTreeByOffsetReader =
      new BufferedTransactionPointwiseReader[(Offset, Set[Party]), GetTransactionTreeResponse](
        fetchFromPersistence = {
          case (
                (offset, parties),
                loggingContext: LoggingContextWithTrace,
              ) =>
            delegate.lookupTransactionTreeByOffset(
              offset = offset,
              requestingParties = parties,
            )(loggingContext)
        },
        fetchFromBuffer = queryParam => transactionsBuffer.lookup(queryParam._1),
        toApiResponse = (
            transactionAccepted: TransactionLogUpdate.TransactionAccepted,
            queryParam: (Offset, Set[Party]),
            loggingContext: LoggingContextWithTrace,
        ) =>
          ToTransactionTree.toGetTransactionResponse(
            transactionAccepted,
            queryParam._2,
            lfValueTranslation,
          )(loggingContext, directEC),
      )

    new BufferedUpdateReader(
      delegate = delegate,
      bufferedUpdatesReader = UpdatesStreamReader,
      bufferedTransactionTreesReader = transactionTreesStreamReader,
      lfValueTranslation = lfValueTranslation,
      bufferedTransactionByIdReader = bufferedTransactionByIdReader,
      bufferedTransactionTreeByIdReader = bufferedTransactionTreeByIdReader,
      bufferedTransactionByOffsetReader = bufferedTransactionByOffsetReader,
      bufferedTransactionTreeByOffsetReader = bufferedTransactionTreeByOffsetReader,
      directEC = directEC,
    )
  }
}
