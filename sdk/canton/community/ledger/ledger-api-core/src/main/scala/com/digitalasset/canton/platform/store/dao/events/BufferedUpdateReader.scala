// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.update_service.{GetUpdateResponse, GetUpdatesResponse}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.dao.BufferedStreamsReader.FetchFromPersistence
import com.digitalasset.canton.platform.store.dao.events.TransactionLogUpdatesConversions
import com.digitalasset.canton.platform.store.dao.{
  BufferedStreamsReader,
  BufferedUpdatePointwiseReader,
  EventProjectionProperties,
  LedgerDaoUpdateReader,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.{InternalUpdateFormat, TemplatePartiesFilter}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

private[events] class BufferedUpdateReader(
    delegate: LedgerDaoUpdateReader,
    bufferedUpdatesReader: BufferedStreamsReader[InternalUpdateFormat, GetUpdatesResponse],
    bufferedUpdateReader: BufferedUpdatePointwiseReader[
      (LookupKey, InternalUpdateFormat),
      GetUpdateResponse,
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
        bufferFilter = TransactionLogUpdatesConversions
          .filter(internalUpdateFormat),
        toApiResponse = TransactionLogUpdatesConversions
          .toGetUpdatesResponse(internalUpdateFormat, lfValueTranslation)(
            loggingContext,
            directEC,
          ),
      )

  def lookupUpdateBy(
      lookupKey: LookupKey,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetUpdateResponse]] =
    Future.delegate(bufferedUpdateReader.fetch(lookupKey -> internalUpdateFormat))

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
      updatesBuffer: InMemoryFanoutBuffer,
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

    val updatesStreamReader =
      new BufferedStreamsReader[InternalUpdateFormat, GetUpdatesResponse](
        inMemoryFanoutBuffer = updatesBuffer,
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

    val updatePointwiseReader =
      new BufferedUpdatePointwiseReader[
        (LookupKey, InternalUpdateFormat),
        GetUpdateResponse,
      ](
        fetchFromPersistence = {
          case (
                (lookupKey, internalUpdateFormat),
                loggingContext: LoggingContextWithTrace,
              ) =>
            delegate.lookupUpdateBy(
              lookupKey = lookupKey,
              internalUpdateFormat = internalUpdateFormat,
            )(loggingContext)
        },
        fetchFromBuffer = queryParam => updatesBuffer.lookup(queryParam._1),
        toApiResponse = (
            transactionLogUpdate: TransactionLogUpdate,
            queryParam: (LookupKey, InternalUpdateFormat),
            loggingContext: LoggingContextWithTrace,
        ) =>
          TransactionLogUpdatesConversions.toGetUpdateResponse(
            transactionLogUpdate,
            queryParam._2,
            lfValueTranslation,
          )(loggingContext, directEC),
      )

    new BufferedUpdateReader(
      delegate = delegate,
      bufferedUpdatesReader = updatesStreamReader,
      bufferedUpdateReader = updatePointwiseReader,
      lfValueTranslation = lfValueTranslation,
      directEC = directEC,
    )
  }
}
