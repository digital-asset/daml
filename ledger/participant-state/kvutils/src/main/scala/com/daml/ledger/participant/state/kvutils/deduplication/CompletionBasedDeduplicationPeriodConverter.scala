// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

import java.time.{Duration, Instant}

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.index.v2.IndexCompletionsService
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.participant.util.HexOffset

import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits._

class CompletionBasedDeduplicationPeriodConverter(
    completionService: IndexCompletionsService
) extends DeduplicationPeriodConverter {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def convertOffsetToDuration(
      offset: Ref.HexString,
      applicationId: Ref.ApplicationId,
      readers: Set[Ref.Party],
      maxRecordTime: Instant,
  )(implicit
      mat: Materializer,
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[DeduplicationConversionFailure, Duration]] = completionAtOffset(
    applicationId,
    readers,
    offset,
  ).map {
    case Some(CompletionStreamResponse(Some(checkpoint), _)) =>
      if (checkpoint.offset.flatMap(_.value.absolute).contains(offset)) {
        checkpoint.recordTime match {
          case Some(recordTime) =>
            val duration = Duration.between(recordTime.asJavaInstant, maxRecordTime)
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

  private def completionAtOffset(
      applicationId: Ref.ApplicationId,
      readers: Set[Ref.Party],
      offset: Ref.HexString,
  )(implicit
      mat: Materializer,
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Option[CompletionStreamResponse]] = {
    val previousOffset = HexOffset.previous(offset)
    val completionOffset = LedgerOffset.Absolute(offset)
    completionService.currentLedgerEnd().flatMap { ledgerEnd =>
      if (ledgerEnd < completionOffset) {
        logger
          .debug(s"Requested offset '$offset' for completion which is beyond current ledger end.")
        Future.successful(None)
      } else {
        completionService
          .getCompletions(
            startExclusive =
              previousOffset.map(LedgerOffset.Absolute).getOrElse(LedgerOffset.LedgerBegin),
            endInclusive = completionOffset,
            applicationId = applicationId,
            parties = readers,
          )
          .runWith(Sink.headOption)
      }
    }
  }

}
