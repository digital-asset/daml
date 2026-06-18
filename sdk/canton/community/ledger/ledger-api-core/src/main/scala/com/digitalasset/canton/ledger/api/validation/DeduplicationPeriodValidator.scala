// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.digitalasset.canton.logging.ErrorLoggingContext
import io.grpc.StatusRuntimeException

import java.time.Duration

object DeduplicationPeriodValidator {
  private val fieldName = "deduplication_period"

  def validateNonNegativeDuration(duration: Duration)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Duration] = if (duration.isNegative)
    Left(
      ValidationErrors
        .invalidField(
          fieldName,
          "Duration must be positive",
        )
    )
  else Right(duration)
}
