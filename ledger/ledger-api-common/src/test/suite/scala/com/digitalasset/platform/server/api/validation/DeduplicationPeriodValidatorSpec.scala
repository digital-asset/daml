// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import java.time
import java.time.Duration

import com.daml.error.definitions.LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField.ValidMaxDeduplicationFieldKey
import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.DeduplicationPeriod.DeduplicationDuration
import com.daml.ledger.api.validation.ValidatorTestUtils
import io.grpc.Status.Code.FAILED_PRECONDITION
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class DeduplicationPeriodValidatorSpec
    extends AnyWordSpec
    with Matchers
    with ValidatorTestUtils
    with TableDrivenPropertyChecks {

  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger = NoLogging
  private val maxDeduplicationDuration = time.Duration.ofSeconds(5)

  "not allow deduplication duration exceeding maximum deduplication duration" in {
    val durationSecondsExceedingMax = maxDeduplicationDuration.plusSeconds(1).getSeconds
    requestMustFailWith(
      request = DeduplicationPeriodValidator.validate(
        DeduplicationDuration(
          Duration.ofSeconds(durationSecondsExceedingMax)
        ),
        maxDeduplicationDuration,
      ),
      code = FAILED_PRECONDITION,
      description = s"INVALID_DEDUPLICATION_PERIOD(9,0): The submitted command had an invalid deduplication period: The given deduplication duration of ${java.time.Duration
          .ofSeconds(durationSecondsExceedingMax)} exceeds the maximum deduplication duration of ${maxDeduplicationDuration}",
      metadata = Map(
        ValidMaxDeduplicationFieldKey -> maxDeduplicationDuration.toString
      ),
    )
  }
}
