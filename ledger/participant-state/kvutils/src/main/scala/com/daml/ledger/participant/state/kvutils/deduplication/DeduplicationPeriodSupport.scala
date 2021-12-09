// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

import java.time.{Duration, Instant}

import akka.stream.Materializer
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.domain.ApplicationId
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.lf.data.{Ref, Time}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.{DeduplicationPeriodValidator, ErrorFactories}

import scala.concurrent.{ExecutionContext, Future}

class DeduplicationPeriodSupport(
    converter: DeduplicationPeriodConverter,
    validation: DeduplicationPeriodValidator,
    errorFactories: ErrorFactories,
) {

  private val logger = ContextualizedLogger.get(this.getClass)

  def supportedDeduplicationPeriod(
      deduplicationPeriod: DeduplicationPeriod,
      maxDeduplicationDuration: Duration,
      timeModel: LedgerTimeModel,
      applicationId: ApplicationId,
      actAs: Set[Ref.Party],
      submittedAt: Instant,
  )(implicit
      mat: Materializer,
      ec: ExecutionContext,
      loggingContext: LoggingContext,
      contextualizedErrorLogger: ContextualizedErrorLogger,
  ): Future[DeduplicationPeriod] = {
    val validatedDeduplicationPeriod = deduplicationPeriod match {
      case period: DeduplicationPeriod.DeduplicationDuration =>
        Future.successful(Right(period))
      case DeduplicationPeriod.DeduplicationOffset(offset) =>
        converter
          .convertOffsetToDuration(
            offset.toHexString,
            applicationId,
            actAs,
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
                    errorFactories.invalidDeduplicationDuration(
                      "deduplication_period",
                      s"Cannot convert deduplication offset to duration because there is no completion at given offset $offset.",
                      Some(false),
                      None,
                    )
                  )
              },
              duration => {
                logger.debug(s"Converted deduplication offset $offset to duration $duration")
                validation.validate(
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
