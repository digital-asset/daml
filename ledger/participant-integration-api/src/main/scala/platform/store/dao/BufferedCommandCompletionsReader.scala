// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.cache.InMemoryFanoutBuffer
import com.daml.platform.store.dao.BufferedCommandCompletionsReader.CompletionsFilter
import com.daml.platform.store.dao.BufferedStreamsReader.FetchFromPersistence
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
    bufferReader.stream(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      persistenceFetchArgs = applicationId -> parties,
      bufferFilter = filterCompletions(_, parties, applicationId),
      toApiResponse = (response: CompletionStreamResponse) => Future.successful(response),
    )

  private def filterCompletions(
      transactionLogUpdate: TransactionLogUpdate,
      parties: Set[Party],
      applicationId: String,
  ): Option[CompletionStreamResponse] = (transactionLogUpdate match {
    case TransactionLogUpdate.TransactionAccepted(_, _, _, _, _, _, Some(completionDetails)) =>
      Some(completionDetails)
    case TransactionLogUpdate.TransactionRejected(_, completionDetails) => Some(completionDetails)
    case TransactionLogUpdate.TransactionAccepted(_, _, _, _, _, _, None) =>
      // Completion details missing highlights submitter is not local to this participant
      None
  }).flatMap(toApiCompletion(_, parties, applicationId))

  private def toApiCompletion(
      completionDetails: CompletionDetails,
      parties: Set[Party],
      applicationId: String,
  ): Option[CompletionStreamResponse] = {
    val completion = completionDetails.completionStreamResponse.completions.headOption
      .getOrElse(throw new RuntimeException("No completion in completion stream response"))

    val visibilityPredicate =
      completion.applicationId == applicationId &&
        parties.iterator.exists(completionDetails.submitters)

    Option.when(visibilityPredicate)(completionDetails.completionStreamResponse)
  }
}

object BufferedCommandCompletionsReader {
  private[dao] type Parties = Set[Party]
  private[dao] type CompletionsFilter = (ApplicationId, Parties)

  def apply(
      delegate: LedgerDaoCommandCompletionsReader,
      inMemoryFanoutBuffer: InMemoryFanoutBuffer,
      metrics: Metrics,
  )(implicit ec: ExecutionContext): BufferedCommandCompletionsReader = {
    val fetchCompletions = new FetchFromPersistence[CompletionsFilter, CompletionStreamResponse] {
      override def apply(
          startExclusive: Offset,
          endInclusive: Offset,
          filter: (ApplicationId, Parties),
      )(implicit
          loggingContext: LoggingContext
      ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
        val (applicationId, parties) = filter
        delegate.getCommandCompletions(startExclusive, endInclusive, applicationId, parties)
      }
    }

    new BufferedCommandCompletionsReader(
      bufferReader = new BufferedStreamsReader[CompletionsFilter, CompletionStreamResponse](
        inMemoryFanoutBuffer = inMemoryFanoutBuffer,
        fetchFromPersistence = fetchCompletions,
        // Processing for completions is a no-op so it is unnecessary to have configurable parallelism.
        bufferedStreamEventsProcessingParallelism = 1,
        metrics = metrics,
        streamName = "completions",
      )
    )
  }
}
