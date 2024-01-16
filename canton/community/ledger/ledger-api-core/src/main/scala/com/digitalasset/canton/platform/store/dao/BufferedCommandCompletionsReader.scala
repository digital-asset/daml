// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.dao.BufferedCommandCompletionsReader.CompletionsFilter
import com.digitalasset.canton.platform.store.dao.BufferedStreamsReader.FetchFromPersistence
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate.CompletionDetails
import com.digitalasset.canton.platform.{ApplicationId, Party}
import com.digitalasset.canton.tracing.Traced
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

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
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, CompletionStreamResponse), NotUsed] =
    bufferReader.stream(
      startExclusive = startExclusive,
      endInclusive = endInclusive,
      persistenceFetchArgs = applicationId -> parties,
      bufferFilter = filterCompletions(_, parties, applicationId),
      toApiResponse = (response: CompletionStreamResponse) => Future.successful(response),
      multiDomainEnabled = false, // for completions it does not matter
    )

  private def filterCompletions(
      transactionLogUpdate: Traced[TransactionLogUpdate],
      parties: Set[Party],
      applicationId: String,
  ): Option[CompletionStreamResponse] = (transactionLogUpdate.value match {
    case TransactionLogUpdate.TransactionAccepted(_, _, _, _, _, _, Some(completionDetails), _) =>
      Some(completionDetails)
    case TransactionLogUpdate.TransactionRejected(_, completionDetails) => Some(completionDetails)
    case TransactionLogUpdate.TransactionAccepted(_, _, _, _, _, _, None, _) =>
      // Completion details missing highlights submitter is not local to this participant
      None
    case u: TransactionLogUpdate.ReassignmentAccepted => u.completionDetails
  }).flatMap(toApiCompletion(_, parties, applicationId))

  private def toApiCompletion(
      completionDetails: CompletionDetails,
      parties: Set[Party],
      applicationId: String,
  ): Option[CompletionStreamResponse] = {
    val completion = completionDetails.completionStreamResponse.completion
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
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): BufferedCommandCompletionsReader = {
    val fetchCompletions = new FetchFromPersistence[CompletionsFilter, CompletionStreamResponse] {
      override def apply(
          startExclusive: Offset,
          endInclusive: Offset,
          filter: (ApplicationId, Parties),
          multiDomainEnabled: Boolean,
      )(implicit
          loggingContext: LoggingContextWithTrace
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
        loggerFactory,
      )
    )
  }
}
