// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.model.Test.Dummy
import io.grpc.Status

class LedgerConfigurationServiceIT extends LedgerTestSuite {
  test("ConfigSucceeds", "Return a valid configuration for a valid request", allocate(NoParties))(
    implicit ec => { case Participants(Participant(ledger)) =>
      for {
        config <- ledger.configuration()
      } yield {
        assert(
          config.maxDeduplicationTime.isDefined,
          "The maxDeduplicationTime field of the configuration is empty",
        )
      }
    }
  )

  test("ConfigLedgerId", "Return NOT_FOUND to invalid ledger identifier", allocate(NoParties))(
    implicit ec => { case Participants(Participant(ledger)) =>
      val invalidLedgerId = "THIS_IS_AN_INVALID_LEDGER_ID"
      for {
        failure <- ledger
          .configuration(overrideLedgerId = Some(invalidLedgerId))
          .mustFail("retrieving ledger configuration with an invalid ledger ID")
      } yield {
        assertGrpcError(
          failure,
          Status.Code.NOT_FOUND,
          Some(s"Ledger ID '$invalidLedgerId' not found."),
        )
      }
    }
  )

  test(
    "CSLSuccessIfMaxDedplicationTimeRight",
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
    "CSLSuccessIfMaxDeduplicationTimeExceeded",
    "Submission returns INVALID_ARGUMENT if deduplication time is too big",
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
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, exceptionMessageSubstring = None)
    }
  })
}
