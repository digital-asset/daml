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
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.ledger.api.{ParticipantAuthorizationFormat, TopologyFormat}
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
  LedgerDaoTransactionsReader,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.{
  InternalEventFormat,
  InternalTransactionFormat,
  InternalUpdateFormat,
  Party,
  TemplatePartiesFilter,
}
import com.digitalasset.canton.{data, platform}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

private[events] class BufferedTransactionsReader(
    experimentalEnableTopologyEvents: Boolean,
    delegate: LedgerDaoTransactionsReader,
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
    extends LedgerDaoTransactionsReader {

  override def getFlatTransactions(
      startInclusive: Offset,
      endInclusive: Offset,
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
    val internalUpdateFormat = InternalUpdateFormat(
      includeTransactions = Some(
        InternalTransactionFormat(
          internalEventFormat = InternalEventFormat(
            templatePartiesFilter = filter,
            eventProjectionProperties = eventProjectionProperties,
          ),
          transactionShape = AcsDelta,
        )
      ),
      includeReassignments = Some(
        InternalEventFormat(
          templatePartiesFilter = filter,
          eventProjectionProperties = eventProjectionProperties,
        )
      ),
      includeTopologyEvents = Some(
        TopologyFormat(
          Some(
            ParticipantAuthorizationFormat(
              parties = filter.allFilterParties
            )
          )
        )
      ),
    )

    bufferedUpdatesReader
      .stream(
        startInclusive = startInclusive,
        endInclusive = endInclusive,
        persistenceFetchArgs = internalUpdateFormat,
        bufferFilter = ToFlatTransaction
          .filter(internalUpdateFormat)
          .andThen {
            case Some(TransactionLogUpdate.TopologyTransactionEffective(_, _, _, _, _))
                if !experimentalEnableTopologyEvents =>
              None
            case something => something
          },
        toApiResponse = ToFlatTransaction
          .toGetUpdatesResponse(internalUpdateFormat, lfValueTranslation)(
            loggingContext,
            directEC,
          ),
      )
  }

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
          .filter(requestingParties)
          .andThen {
            case Some(TransactionLogUpdate.TopologyTransactionEffective(_, _, _, _, _))
                if !experimentalEnableTopologyEvents =>
              None
            case something => something
          },
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
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] = {
    val internalTransactionFormat = InternalTransactionFormat(
      internalEventFormat = InternalEventFormat(
        templatePartiesFilter = TemplatePartiesFilter(
          relation = Map.empty,
          templateWildcardParties = Some(requestingParties),
        ),
        eventProjectionProperties = EventProjectionProperties(
          verbose = true,
          templateWildcardWitnesses = Some(requestingParties.map(_.toString)),
        ),
      ),
      transactionShape = AcsDelta,
    )
    Future.delegate(
      bufferedTransactionByIdReader.fetch(updateId -> internalTransactionFormat)
    )
  }

  override def lookupFlatTransactionByOffset(
      offset: data.Offset,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] = {
    val internalTransactionFormat = InternalTransactionFormat(
      internalEventFormat = InternalEventFormat(
        templatePartiesFilter = TemplatePartiesFilter(
          relation = Map.empty,
          templateWildcardParties = Some(requestingParties),
        ),
        eventProjectionProperties = EventProjectionProperties(
          verbose = true,
          templateWildcardWitnesses = Some(requestingParties.map(_.toString)),
        ),
      ),
      transactionShape = AcsDelta,
    )
    Future.delegate(
      bufferedTransactionByOffsetReader.fetch(offset -> internalTransactionFormat)
    )
  }

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

private[platform] object BufferedTransactionsReader {
  def apply(
      delegate: LedgerDaoTransactionsReader,
      transactionsBuffer: InMemoryFanoutBuffer,
      eventProcessingParallelism: Int,
      experimentalEnableTopologyEvents: Boolean,
      lfValueTranslation: LfValueTranslation,
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): BufferedTransactionsReader = {
    val directEC = DirectExecutionContext(
      loggerFactory.getLogger(BufferedTransactionsReader.getClass)
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
          ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
            // TODO FIXME temporarily mapping back until persistence is catching up
            val transactionFormat = filter.includeTransactions
              .getOrElse(
                throw new IllegalStateException(
                  "Include Transactions is expected here ath the moment"
                )
              )
            delegate
              .getFlatTransactions(
                startInclusive = startInclusive,
                endInclusive = endInclusive,
                filter = transactionFormat.internalEventFormat.templatePartiesFilter,
                eventProjectionProperties =
                  transactionFormat.internalEventFormat.eventProjectionProperties,
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

    val bufferedTransactionByIdReader =
      new BufferedTransactionPointwiseReader[
        (String, InternalTransactionFormat),
        GetTransactionResponse,
      ](
        fetchFromPersistence = (
            queryParam: (String, InternalTransactionFormat),
            loggingContext: LoggingContextWithTrace,
        ) =>
          delegate.lookupFlatTransactionById(
            platform.UpdateId.assertFromString(queryParam._1),
            // TODO FIXME mapping to the changed persistence
            queryParam._2.internalEventFormat.templatePartiesFilter.templateWildcardParties
              .getOrElse(throw new IllegalStateException("Currently wildcard parties are required")),
          )(loggingContext),
        fetchFromBuffer = queryParam => transactionsBuffer.lookup(queryParam._1),
        toApiResponse = (
            transactionAccepted: TransactionLogUpdate.TransactionAccepted,
            queryParam: (String, InternalTransactionFormat),
            loggingContext: LoggingContextWithTrace,
        ) =>
          ToFlatTransaction.toGetFlatTransactionResponse(
            transactionAccepted,
            queryParam._2,
            lfValueTranslation,
          )(loggingContext, directEC),
      )

    val bufferedTransactionTreeByIdReader =
      new BufferedTransactionPointwiseReader[(String, Set[Party]), GetTransactionTreeResponse](
        fetchFromPersistence = (
            queryParam: (String, Set[Party]),
            loggingContext: LoggingContextWithTrace,
        ) =>
          delegate.lookupTransactionTreeById(
            platform.UpdateId.assertFromString(queryParam._1),
            queryParam._2,
          )(loggingContext),
        fetchFromBuffer = queryParam => transactionsBuffer.lookup(queryParam._1),
        toApiResponse = (
            transactionAccepted: TransactionLogUpdate.TransactionAccepted,
            queryParam: (String, Set[Party]),
            loggingContext: LoggingContextWithTrace,
        ) =>
          ToTransactionTree.toGetTransactionResponse(
            transactionAccepted,
            queryParam._2,
            lfValueTranslation,
          )(loggingContext, directEC),
      )

    val bufferedTransactionByOffsetReader =
      new BufferedTransactionPointwiseReader[
        (Offset, InternalTransactionFormat),
        GetTransactionResponse,
      ](
        fetchFromPersistence = (
            queryParam: (Offset, InternalTransactionFormat),
            loggingContext: LoggingContextWithTrace,
        ) =>
          delegate.lookupFlatTransactionByOffset(
            queryParam._1,
            // TODO FIXME mapping to the changed persistence
            queryParam._2.internalEventFormat.templatePartiesFilter.templateWildcardParties
              .getOrElse(throw new IllegalStateException("Currently wildcard parties are required")),
          )(loggingContext),
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
        fetchFromPersistence = (
            queryParam: (Offset, Set[Party]),
            loggingContext: LoggingContextWithTrace,
        ) =>
          delegate.lookupTransactionTreeByOffset(
            queryParam._1,
            queryParam._2,
          )(loggingContext),
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

    new BufferedTransactionsReader(
      experimentalEnableTopologyEvents = experimentalEnableTopologyEvents,
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
