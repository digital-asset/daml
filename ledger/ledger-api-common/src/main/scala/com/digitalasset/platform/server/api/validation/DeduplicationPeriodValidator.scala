// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import java.time.Duration

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.DeduplicationPeriod
import io.grpc.StatusRuntimeException

object DeduplicationPeriodValidator {
  private val fieldName = "deduplication_period"

  def validate(
      deduplicationPeriod: DeduplicationPeriod,
      maxDeduplicationDuration: Duration,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, DeduplicationPeriod] = {
    deduplicationPeriod match {
      case DeduplicationPeriod.DeduplicationDuration(duration) =>
        validateDuration(duration, maxDeduplicationDuration).map(_ => deduplicationPeriod)
      case DeduplicationPeriod.DeduplicationOffset(_) => Right(deduplicationPeriod)
    }
  }

  def validateDuration(duration: Duration, maxDeduplicationDuration: Duration)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Duration] =
    validateNonNegativeDuration(duration).flatMap { duration =>
      if (duration.compareTo(maxDeduplicationDuration) > 0)
        Left(
          ErrorFactories.invalidDeduplicationPeriod(
            s"The given deduplication duration of $duration exceeds the maximum deduplication duration of $maxDeduplicationDuration",
            Some(maxDeduplicationDuration),
          )
        )
      else Right(duration)
    }

  def validateNonNegativeDuration(duration: Duration)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Duration] = if (duration.isNegative)
    Left(
      ErrorFactories
        .invalidField(
          fieldName,
          "Duration must be positive",
        )
    )
  else Right(duration)
}
