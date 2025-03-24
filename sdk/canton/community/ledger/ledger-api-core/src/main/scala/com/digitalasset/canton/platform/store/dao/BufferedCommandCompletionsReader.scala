// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.dao.BufferedCommandCompletionsReader.CompletionsFilter
import com.digitalasset.canton.platform.store.dao.BufferedStreamsReader.FetchFromPersistence
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.{Party, UserId}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

class BufferedCommandCompletionsReader(
    bufferReader: BufferedStreamsReader[CompletionsFilter, CompletionStreamResponse]
) extends LedgerDaoCommandCompletionsReader {

  override def getCommandCompletions(
      startInclusive: Offset,
      endInclusive: Offset,
      userId: UserId,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, CompletionStreamResponse), NotUsed] =
    bufferReader
      .stream(
        startInclusive = startInclusive,
        endInclusive = endInclusive,
        persistenceFetchArgs = userId -> parties,
        bufferFilter = filterCompletions(_, parties, userId),
        toApiResponse = (response: CompletionStreamResponse) => Future.successful(response),
      )

  private def filterCompletions(
      transactionLogUpdate: TransactionLogUpdate,
      parties: Set[Party],
      userId: String,
  ): Option[CompletionStreamResponse] = (transactionLogUpdate match {
    case accepted: TransactionLogUpdate.TransactionAccepted => accepted.completionStreamResponse
    case rejected: TransactionLogUpdate.TransactionRejected =>
      Some(rejected.completionStreamResponse)
    case u: TransactionLogUpdate.ReassignmentAccepted => u.completionStreamResponse
    case _: TransactionLogUpdate.TopologyTransactionEffective => None
  }).flatMap(toApiCompletion(_, parties, userId))

  private def toApiCompletion(
      completionStreamResponse: CompletionStreamResponse,
      parties: Set[Party],
      userId: String,
  ): Option[CompletionStreamResponse] = {
    val completion = {
      val originalCompletion = completionStreamResponse.completionResponse.completion
        .getOrElse(throw new RuntimeException("No completion in completion stream response"))
      originalCompletion.withActAs(originalCompletion.actAs.filter(parties.map(_.toString)))
    }

    val visibilityPredicate =
      completion.userId == userId &&
        completion.actAs.nonEmpty

    Option.when(visibilityPredicate)(
      CompletionStreamResponse.defaultInstance.withCompletion(completion)
    )
  }
}

object BufferedCommandCompletionsReader {
  private[dao] type Parties = Set[Party]
  private[dao] type CompletionsFilter = (UserId, Parties)

  def apply(
      delegate: LedgerDaoCommandCompletionsReader,
      inMemoryFanoutBuffer: InMemoryFanoutBuffer,
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): BufferedCommandCompletionsReader = {
    val fetchCompletions = new FetchFromPersistence[CompletionsFilter, CompletionStreamResponse] {
      override def apply(
          startInclusive: Offset,
          endInclusive: Offset,
          filter: (UserId, Parties),
      )(implicit
          loggingContext: LoggingContextWithTrace
      ): Source[(Offset, CompletionStreamResponse), NotUsed] = {
        val (userId, parties) = filter
        delegate
          .getCommandCompletions(
            startInclusive,
            endInclusive,
            userId,
            parties,
          )
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
