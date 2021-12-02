// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  ): Either[StatusRuntimeException, Duration] = if (duration.isNegative)
    Left(
      errorFactories
        .invalidField(fieldName, "Duration must be positive", definiteAnswer = Some(false))
    )
  else if (duration.compareTo(maxDeduplicationDuration) > 0)
    Left(
      errorFactories.invalidDeduplicationDuration(
        fieldName,
        s"The given deduplication duration of $duration exceeds the maximum deduplication time of $maxDeduplicationDuration",
        definiteAnswer = Some(false),
        Some(maxDeduplicationDuration),
      )
    )
  else Right(duration)
}
