// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.cache.{BufferSlice, EventsBuffer}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.CompletionDetails
import com.daml.platform.{ApplicationId, Party}

import akka.NotUsed
import akka.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

class BufferedCommandCompletionsReader(
    transactionsBuffer: EventsBuffer[TransactionLogUpdate],
    delegate: LedgerDaoCommandCompletionsReader,
    metrics: Metrics,
)(implicit ec: ExecutionContext)
    extends LedgerDaoCommandCompletionsReader {
  private val completionsBufferReaderMetrics =
    metrics.daml.services.index.BufferedReader("completions")

  override def getCommandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
    def bufferSource(bufferSlice: Vector[(Offset, CompletionStreamResponse)]) =
      if (bufferSlice.isEmpty) Source.empty
      else
        Source(bufferSlice)
          .map { response =>
            completionsBufferReaderMetrics.fetchedBuffered.inc()
            response
          }

    val filter = filterCompletions(_, parties, applicationId)

    val source = Source
      .unfoldAsync(startExclusive) {
        case scannedToInclusive if scannedToInclusive < endInclusive =>
          Future {
            transactionsBuffer.slice(scannedToInclusive, endInclusive, filter) match {
              case BufferSlice.Inclusive(slice) =>
                val sourceFromBuffer = bufferSource(slice)
                val nextChunkStartExclusive = slice.lastOption.map(_._1).getOrElse(endInclusive)
                Some(nextChunkStartExclusive -> sourceFromBuffer)

              case BufferSlice.LastBufferChunkSuffix(bufferedStartExclusive, slice) =>
                val sourceFromPersistence = delegate
                  .getCommandCompletions(
                    startExclusive,
                    bufferedStartExclusive,
                    applicationId,
                    parties,
                  )
                val resultSource = sourceFromPersistence.concat(bufferSource(slice))
                Some(endInclusive -> resultSource)
            }
          }
        case _ => Future.successful(None)
      }
      .flatMapConcat(identity)

    Timed
      .source(completionsBufferReaderMetrics.fetchTimer, source)
      .map { response =>
        completionsBufferReaderMetrics.fetchedTotal.inc()
        response
      }
  }

  private def filterCompletions(
      transactionLogUpdate: TransactionLogUpdate,
      parties: Set[Party],
      applicationId: String,
  ): Option[CompletionStreamResponse] = transactionLogUpdate match {
    case TransactionLogUpdate.TransactionAccepted(_, _, _, _, _, Some(completionDetails)) =>
      toApiCompletion(completionDetails, parties, applicationId)
    case TransactionLogUpdate.TransactionRejected(_, completionDetails) =>
      toApiCompletion(completionDetails, parties, applicationId)
    case TransactionLogUpdate.TransactionAccepted(_, _, _, _, _, None) =>
      // Completion details missing highlights submitter is not local to this participant
      None
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
