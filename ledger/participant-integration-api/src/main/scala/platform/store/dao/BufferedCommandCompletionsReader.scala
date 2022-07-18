// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.EventsBuffer
import com.daml.platform.store.dao.BufferedCommandCompletionsReader.CompletionsFilter
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.CompletionDetails
import com.daml.platform.{ApplicationId, Party}

import scala.concurrent.{ExecutionContext, Future}

class BufferedCommandCompletionsReader(
    bufferReader: BufferedStreamsReader[CompletionsFilter, CompletionStreamResponse]
) extends LedgerDaoCommandCompletionsReader {

  override def getCommandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, CompletionStreamResponse), NotUsed] =
    bufferReader.streamUsingBuffered(
      startExclusive,
      endInclusive,
      applicationId -> parties,
      bufferSliceFilter = filterCompletions(_, parties, applicationId),
      toApiResponse = (response: CompletionStreamResponse) => Future.successful(response),
    )

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

object BufferedCommandCompletionsReader {
  private[dao] type Parties = Set[Party]
  private[dao] type CompletionsFilter = (ApplicationId, Parties)

  def apply(
      delegate: LedgerDaoCommandCompletionsReader,
      transactionsBuffer: EventsBuffer[TransactionLogUpdate],
      metrics: Metrics,
      eventProcessingParallelism: Int,
  )(implicit ec: ExecutionContext): BufferedCommandCompletionsReader =
    new BufferedCommandCompletionsReader(
      bufferReader = new BufferedStreamsReader[CompletionsFilter, CompletionStreamResponse](
        inMemoryFanoutBuffer = transactionsBuffer,
        persistenceFetch = (start, end, filter) =>
          delegate.getCommandCompletions(start, end, filter._1, filter._2)(_),
        eventProcessingParallelism = eventProcessingParallelism,
        metrics = metrics,
        name = "completions",
      )
    )
}
