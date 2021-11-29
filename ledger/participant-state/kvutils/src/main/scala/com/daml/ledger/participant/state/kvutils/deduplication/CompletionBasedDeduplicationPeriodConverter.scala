// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

import java.time.{Duration, Instant}

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.api.domain.{ApplicationId, LedgerId, LedgerOffset}
import com.daml.ledger.api.messages.command.completion.CompletionRequest
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.index.v2.IndexCompletionsService
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

class CompletionBasedDeduplicationPeriodConverter(
    ledgerId: LedgerId,
    completionService: IndexCompletionsService,
) extends DeduplicationPeriodConverter {

  override def convertOffsetToDuration(
      offset: Ref.LedgerString,
      applicationId: ApplicationId,
      actAs: Set[Ref.Party],
      submittedAt: Instant,
  )(implicit
      mat: Materializer,
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[DeduplicationConversionFailure, Duration]] = {
    completionAtOffset(
      CompletionRequest(
        ledgerId,
        applicationId,
        actAs,
        LedgerOffset.Absolute(offset),
      )
    ).map {
      case Some(CompletionStreamResponse(Some(checkpoint), _)) =>
        if (checkpoint.offset.flatMap(_.value.absolute).contains(offset)) {
          checkpoint.recordTime match {
            case Some(recordTime) =>
              val duration = Duration.between(recordTime.asJavaInstant, submittedAt)
              Right(duration)
            case None => Left(DeduplicationConversionFailure.CompletionRecordTimeNotAvailable)
          }
        } else {
          Left(DeduplicationConversionFailure.CompletionOffsetNotMatching)
        }
      case Some(CompletionStreamResponse(None, _)) =>
        Left(DeduplicationConversionFailure.CompletionCheckpointNotAvailable)
      case None => Left(DeduplicationConversionFailure.CompletionAtOffsetNotFound)
    }
  }

  private def completionAtOffset(
      request: CompletionRequest
  )(implicit
      mat: Materializer,
      loggingContext: LoggingContext,
  ): Future[Option[CompletionStreamResponse]] = {
    completionService
      .getCompletions(request.offset, request.offset, request.applicationId, request.parties)
      .runWith(Sink.headOption)
  }
}
