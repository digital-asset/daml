// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.metrics.Timed
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer.BufferSlice
import com.digitalasset.canton.platform.store.dao.BufferedStreamsReader.FetchFromPersistence
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.tracing.Traced
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

/** Generic class that helps serving Ledger API streams (e.g. transactions, completions)
  *  from either the in-memory fan-out buffer or from persistence depending on the requested offset range.
  *
  * @param inMemoryFanoutBuffer The in-memory fan-out buffer.
  * @param fetchFromPersistence Fetch stream events from persistence.
  * @param bufferedStreamEventsProcessingParallelism The processing parallelism for buffered elements payloads to API responses.
  * @param metrics Daml metrics.
  * @param streamName The name of a Ledger API stream. Used as a discriminator in metric registry names construction.
  * @param executionContext The execution context
  * @tparam PERSISTENCE_FETCH_ARGS The Ledger API streams filter type of fetches from persistence.
  * @tparam API_RESPONSE The API stream response type.
  */
class BufferedStreamsReader[PERSISTENCE_FETCH_ARGS, API_RESPONSE](
    inMemoryFanoutBuffer: InMemoryFanoutBuffer,
    fetchFromPersistence: FetchFromPersistence[PERSISTENCE_FETCH_ARGS, API_RESPONSE],
    bufferedStreamEventsProcessingParallelism: Int,
    metrics: Metrics,
    streamName: String,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val directEc = DirectExecutionContext(noTracingLogger)

  private val bufferReaderMetrics = metrics.services.index.BufferedReader(streamName)

  /** Serves processed and filtered events from the buffer, with fallback to persistence fetches
    * if the bounds are not within the buffer range bounds.
    *
    * @param startExclusive The start exclusive offset of the search range.
    * @param endInclusive The end inclusive offset of the search range.
    * @param persistenceFetchArgs The filter used for fetching the Ledger API stream responses from persistence.
    * @param bufferFilter The filter used for filtering when searching within the buffer.
    * @param toApiResponse To Ledger API stream response converter.
    * @param loggingContext The logging context.
    * @tparam BUFFER_OUT The output type of elements retrieved from the buffer.
    * @return The Ledger API stream source.
    */
  def stream[BUFFER_OUT](
      startExclusive: Offset,
      endInclusive: Offset,
      persistenceFetchArgs: PERSISTENCE_FETCH_ARGS,
      bufferFilter: Traced[TransactionLogUpdate] => Option[BUFFER_OUT],
      toApiResponse: BUFFER_OUT => Future[API_RESPONSE],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, API_RESPONSE), NotUsed] = {
    def toApiResponseStream(
        slice: Vector[(Offset, BUFFER_OUT)]
    ): Source[(Offset, API_RESPONSE), NotUsed] =
      if (slice.isEmpty) Source.empty
      else
        Source(slice)
          .mapAsync(bufferedStreamEventsProcessingParallelism) { case (offset, payload) =>
            bufferReaderMetrics.fetchedBuffered.inc()
            Timed.future(
              bufferReaderMetrics.conversion,
              toApiResponse(payload).map(offset -> _)(directEc),
            )
          }

    val source = Source
      .unfoldAsync(startExclusive) {
        case scannedToInclusive if scannedToInclusive < endInclusive =>
          Future {
            val bufferSlice = Timed.value(
              bufferReaderMetrics.slice,
              inMemoryFanoutBuffer.slice(
                startExclusive = scannedToInclusive,
                endInclusive = endInclusive,
                filter = bufferFilter,
              ),
            )

            bufferReaderMetrics.sliceSize.update(bufferSlice.slice.size)(MetricsContext.Empty)

            bufferSlice match {
              case BufferSlice.Inclusive(slice) =>
                val apiResponseSource = toApiResponseStream(slice)
                val nextSliceStartExclusive = slice.lastOption.map(_._1).getOrElse(endInclusive)
                Some(nextSliceStartExclusive -> apiResponseSource)

              case BufferSlice.LastBufferChunkSuffix(bufferedStartExclusive, slice) =>
                val sourceFromBuffer =
                  fetchFromPersistence(
                    startExclusive = scannedToInclusive,
                    endInclusive = bufferedStartExclusive,
                    filter = persistenceFetchArgs,
                  )(loggingContext)
                    .concat(toApiResponseStream(slice))
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
  trait FetchFromPersistence[FILTER, API_RESPONSE] {
    def apply(
        startExclusive: Offset,
        endInclusive: Offset,
        filter: FILTER,
    )(implicit loggingContext: LoggingContextWithTrace): Source[(Offset, API_RESPONSE), NotUsed]
  }
}
