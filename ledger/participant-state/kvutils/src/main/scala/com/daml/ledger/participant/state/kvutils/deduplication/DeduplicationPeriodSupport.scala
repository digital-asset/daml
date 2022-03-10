// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

import java.time.{Duration, Instant}

import akka.stream.Materializer
import com.daml.error.ContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.lf.data.{Ref, Time}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.DeduplicationPeriodValidator

import scala.concurrent.{ExecutionContext, Future}

class DeduplicationPeriodSupport(
    converter: DeduplicationPeriodConverter
) {

  private val logger = ContextualizedLogger.get(this.getClass)

  def supportedDeduplicationPeriod(
      deduplicationPeriod: DeduplicationPeriod,
      maxDeduplicationDuration: Duration,
      timeModel: LedgerTimeModel,
      applicationId: Ref.ApplicationId,
      readers: Set[Ref.Party],
      submittedAt: Instant,
  )(implicit
      mat: Materializer,
      ec: ExecutionContext,
      loggingContext: LoggingContext,
      contextualizedErrorLogger: ContextualizedErrorLogger,
  ): Future[DeduplicationPeriod] = {
    val validatedDeduplicationPeriod = deduplicationPeriod match {
      case period: DeduplicationPeriod.DeduplicationDuration =>
        Future { DeduplicationPeriodValidator.validate(period, maxDeduplicationDuration) }
      case DeduplicationPeriod.DeduplicationOffset(offset) =>
        logger.debug(s"Converting deduplication period offset $offset to duration")
        converter
          .convertOffsetToDuration(
            offset.toHexString,
            applicationId,
            readers,
            timeModel.maxRecordTime(Time.Timestamp.assertFromInstant(submittedAt)).toInstant,
          )
          .map(
            _.fold(
              {
                case reason @ (DeduplicationConversionFailure.CompletionAtOffsetNotFound |
                    DeduplicationConversionFailure.CompletionOffsetNotMatching |
                    DeduplicationConversionFailure.CompletionRecordTimeNotAvailable |
                    DeduplicationConversionFailure.CompletionCheckpointNotAvailable) =>
                  logger.warn(
                    s"Failed to convert deduplication offset $offset to duration: $reason"
                  )
                  Left(
                    LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField
                      .Reject(
                        s"Cannot convert deduplication offset to duration because there is no completion at given offset $offset.",
                        None,
                      )
                      .asGrpcError
                  )
              },
              duration => {
                logger.debug(s"Converted deduplication offset $offset to duration $duration")
                // We implicitly extend the deduplication period slightly:
                // If a later offset has the same record time as `offset` (e.g., in static time mode),
                // command deduplication must consider this later offset.
                // Yet, a deduplication duration cannot distinguish between offsets with the same record time.
                // We therefore extend the deduplication period to include all offsets with the same record time
                // as `offset`, including `offset` itself which would not have to be included in the deduplication period.
                // This is allowed as the ledger implementation may extend the deduplication period.
                DeduplicationPeriodValidator.validate(
                  DeduplicationPeriod.DeduplicationDuration(duration),
                  maxDeduplicationDuration,
                )
              },
            )
          )
    }
    validatedDeduplicationPeriod.map(_.fold(throw _, identity))
  }

}
