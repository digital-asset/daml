// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.cache.{BufferSlice, EventsBuffer}
import com.daml.platform.store.dao.BufferedStreamsReader.PersistenceFetch
import com.daml.platform.store.interfaces.TransactionLogUpdate

import scala.concurrent.{ExecutionContext, Future}

class BufferedStreamsReader[FILTER, API_RESPONSE](
    transactionsBuffer: EventsBuffer[TransactionLogUpdate],
    persistenceFetch: PersistenceFetch[FILTER, API_RESPONSE],
    eventProcessingParallelism: Int,
    metrics: Metrics,
    name: String,
)(implicit executionContext: ExecutionContext) {
  private val bufferReaderMetrics = metrics.daml.services.index.BufferedReader(name)

  def getEvents[BUFFER_OUT](
      startExclusive: Offset,
      endInclusive: Offset,
      persistenceFetchFilter: FILTER,
      bufferSliceFilter: TransactionLogUpdate => Option[BUFFER_OUT],
      toApiResponse: BUFFER_OUT => Future[API_RESPONSE],
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, API_RESPONSE), NotUsed] = {
    def bufferSource(
        bufferSlice: Vector[(Offset, BUFFER_OUT)]
    ): Source[(Offset, API_RESPONSE), NotUsed] =
      if (bufferSlice.isEmpty) Source.empty
      else
        Source(bufferSlice)
          .mapAsync(eventProcessingParallelism) { case (offset, payload) =>
            bufferReaderMetrics.fetchedBuffered.inc()
            Timed.future(
              bufferReaderMetrics.conversion,
              toApiResponse(payload).map(offset -> _)(ExecutionContext.parasitic),
            )
          }

    val source = Source
      .unfoldAsync(startExclusive) {
        case scannedToInclusive if scannedToInclusive < endInclusive =>
          Future {
            transactionsBuffer.slice(scannedToInclusive, endInclusive, bufferSliceFilter) match {
              case BufferSlice.Inclusive(slice) =>
                val sourceFromBuffer = bufferSource(slice)
                val nextChunkStartExclusive = slice.lastOption.map(_._1).getOrElse(endInclusive)
                Some(nextChunkStartExclusive -> sourceFromBuffer)

              case BufferSlice.LastBufferChunkSuffix(bufferedStartExclusive, slice) =>
                val sourceFromBuffer =
                  persistenceFetch(startExclusive, bufferedStartExclusive, persistenceFetchFilter)(
                    loggingContext
                  )
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
}

private[platform] object BufferedStreamsReader {
  type PersistenceFetch[FILTER, API_RESPONSE] =
    (Offset, Offset, FILTER) => LoggingContext => Source[(Offset, API_RESPONSE), NotUsed]
}
