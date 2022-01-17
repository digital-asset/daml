// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import java.time.Duration

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.DeduplicationPeriod
import io.grpc.StatusRuntimeException

class DeduplicationPeriodValidator(
    errorFactories: ErrorFactories
) {
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
          errorFactories.invalidDeduplicationPeriod(
            fieldName,
            s"The given deduplication duration of $duration exceeds the maximum deduplication time of $maxDeduplicationDuration",
            definiteAnswer = Some(false),
            Some(maxDeduplicationDuration),
          )
        )
      else Right(duration)
    }

  def validateNonNegativeDuration(duration: Duration)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Duration] = if (duration.isNegative)
    Left(
      errorFactories
        .invalidDeduplicationPeriod(
          fieldName,
          "Duration must be positive",
          definiteAnswer = Some(false),
          None,
        )
    )
  else Right(duration)
}
