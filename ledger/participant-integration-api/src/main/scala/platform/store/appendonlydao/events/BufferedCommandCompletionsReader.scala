// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref.ApplicationId
import com.daml.logging.LoggingContext
import com.daml.metrics.{InstrumentedSource, Metrics, Timed}
import com.daml.platform.store.appendonlydao.LedgerDaoCommandCompletionsReader
import com.daml.platform.store.cache.{BufferSlice, EventsBuffer}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.{CompletionDetails, LedgerEndMarker}

class BufferedCommandCompletionsReader(
    completionsBuffer: EventsBuffer[TransactionLogUpdate],
    delegate: LedgerDaoCommandCompletionsReader,
    metrics: Metrics,
) extends LedgerDaoCommandCompletionsReader {
  private val outputStreamBufferSize = 128
  private val completionsBufferMetrics = metrics.daml.services.index.BufferReader("completions")

  override def getCommandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
    def bufferedSource(slice: Array[(Offset, TransactionLogUpdate)]) =
      Source
        .fromIterator(() => slice.iterator)
        .collect {
          case (
                offset,
                TransactionLogUpdate.TransactionAccepted(_, _, _, _, _, Some(completionDetails)),
              ) =>
            offset -> toApiCompletion(completionDetails, parties, applicationId)
          case (offset, rejected: TransactionLogUpdate.SubmissionRejected) =>
            offset -> toApiCompletion(rejected.completionDetails, parties, applicationId)
          case (_, _: LedgerEndMarker) =>
            throw new RuntimeException("Shouldn't make it in the buffer")
        }
        .collect { case (offset, Some(completion)) =>
          completionsBufferMetrics.fetchedBuffered.inc()
          offset -> completion
        }

    def getNextChunk(
        startExclusive: Offset
    ): () => Source[(Offset, CompletionStreamResponse), NotUsed] = () =>
      getCommandCompletions(
        startExclusive,
        endInclusive,
        applicationId,
        parties,
      )

    val transactionsSource = Timed.source(
      completionsBufferMetrics.fetchTimer, {
        completionsBuffer.slice(startExclusive, endInclusive) match {
          case BufferSlice.Empty =>
            delegate.getCommandCompletions(startExclusive, endInclusive, applicationId, parties)

          case BufferSlice.EmptyPrefix =>
            delegate.getCommandCompletions(startExclusive, endInclusive, applicationId, parties)

          case BufferSlice.Prefix(head, tail, isChunked) =>
            if (tail.length == 0)
              delegate.getCommandCompletions(startExclusive, endInclusive, applicationId, parties)
            else
              delegate
                .getCommandCompletions(startExclusive, head._1, applicationId, parties)
                .concat(bufferedSource(tail))
                .concatLazy {
                  if (isChunked) Source.lazySource(getNextChunk(tail.last._1)) else Source.empty
                }

          case BufferSlice.Inclusive(slice, isChunked) =>
            bufferedSource(slice)
              .concatLazy {
                if (isChunked) Source.lazySource(getNextChunk(slice.last._1)) else Source.empty
              }
        }
      }.map(tx => {
        completionsBufferMetrics.fetchedTotal.inc()
        tx
      }),
    )

    InstrumentedSource.bufferedSource(
      original = transactionsSource,
      counter = completionsBufferMetrics.bufferSize,
      size = outputStreamBufferSize,
    )
  }

  private def toApiCompletion(
      completion: CompletionDetails,
      parties: Set[Party],
      applicationId: String,
  ): Option[CompletionStreamResponse] = {
    val completionHead = completion.completionStreamResponse.completions.headOption
      .getOrElse(throw new RuntimeException("Completions must not be empty"))
    if (
      completionHead.applicationId == applicationId && parties.iterator
        .exists(completion.submitters)
    )
      Some(completion.completionStreamResponse)
    else None
  }
}
