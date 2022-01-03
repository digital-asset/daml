// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import java.util.regex.Pattern

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.model.Test.Dummy
import io.grpc.Status

class CommandDeduplicationPeriodValidationIT extends LedgerTestSuite {

  test(
    "ValidDeduplicationDuration",
    "Submission returns OK if deduplication time is within the accepted interval",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    // Submission using the maximum allowed deduplication time
    val request = ledger.submitRequest(party, Dummy(party).create.command)
    for {
      config <- ledger.configuration()
      maxDedupTime = config.maxDeduplicationTime.get
      _ <- ledger.submit(request.update(_.commands.deduplicationTime := maxDedupTime))
    } yield {
      // No assertions to make, since the command went through as expected
    }
  })

  test(
    "DeduplicationDurationExceedsMaxDeduplicationDuration",
    "Submission returns expected error codes if deduplication time is too big",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.submitRequest(party, Dummy(party).create.command)
    for {
      config <- ledger.configuration()
      maxDedupTime = config.maxDeduplicationTime.get
      failure <- ledger
        .submit(
          request.update(
            _.commands.deduplicationTime := maxDedupTime.update(
              _.seconds := maxDedupTime.seconds + 1
            )
          )
        )
        .mustFail("submitting a command with a deduplication time that is too big")
    } yield {
      val expectedCode =
        if (ledger.features.selfServiceErrorCodes) Status.Code.FAILED_PRECONDITION
        else Status.Code.INVALID_ARGUMENT
      val expectedError =
        if (ledger.features.selfServiceErrorCodes)
          LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField
        else LedgerApiErrors.RequestValidation.InvalidField
      assertGrpcErrorRegex(
        ledger,
        failure,
        expectedCode,
        expectedError,
        Some(
          Pattern.compile(
            "The given deduplication .+ exceeds the maximum deduplication .+"
          )
        ),
      )
    }
  })
}
