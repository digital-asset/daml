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

/** Generic class that helps serving Ledger API streams (e.g. transactions, completions)
  *  from either the in-memory fan-out buffer or from persistence.
  *
  * @param inMemoryFanoutBuffer The in-memory fan-out buffer.
  * @param persistenceFetch Fetch stream events from persistence.
  * @param eventProcessingParallelism The processing parallelism for converting payloads to API responses.
  * @param metrics Daml metrics.
  * @param name The name of the Ledger API stream. Used as a discriminator in metric registry names construction.
  * @param executionContext The execution context
  * @tparam PERSISTENCE_FETCH_FILTER The Ledger API streams filter type of fetches from persistence.
  * @tparam API_RESPONSE The API stream response type.
  */
class BufferedStreamsReader[PERSISTENCE_FETCH_FILTER, API_RESPONSE](
    inMemoryFanoutBuffer: EventsBuffer[TransactionLogUpdate],
    persistenceFetch: PersistenceFetch[PERSISTENCE_FETCH_FILTER, API_RESPONSE],
    eventProcessingParallelism: Int,
    metrics: Metrics,
    name: String,
)(implicit executionContext: ExecutionContext) {
  private val bufferReaderMetrics = metrics.daml.services.index.BufferedReader(name)

  /** Serves processed and filtered events from the buffer.
    *
    * @param startExclusive The start exclusive offset of the search range.
    * @param endInclusive The end inclusive offset of the search range.
    * @param persistenceFetchFilter The filter used for fetching the Ledger API stream responses from persistence.
    * @param bufferSliceFilter The filter used for filtering when searching within the buffer.
    * @param toApiResponse To Ledger API stream response converter.
    * @param loggingContext The logging context.
    * @tparam BUFFER_OUT The output type of elements retrieved from the buffer.
    * @return The Ledger API stream source.
    */
  def getEvents[BUFFER_OUT](
      startExclusive: Offset,
      endInclusive: Offset,
      persistenceFetchFilter: PERSISTENCE_FETCH_FILTER,
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
            inMemoryFanoutBuffer.slice(scannedToInclusive, endInclusive, bufferSliceFilter) match {
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
