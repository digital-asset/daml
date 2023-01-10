// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite

class LedgerConfigurationServiceIT extends LedgerTestSuite {
  test("ConfigSucceeds", "Return a valid configuration for a valid request", allocate(NoParties))(
    implicit ec => { case Participants(Participant(ledger)) =>
      for {
        config <- ledger.configuration()
      } yield {
        assert(
          config.maxDeduplicationDuration.isDefined,
          "The maxDeduplicationDuration field of the configuration is empty",
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
          LedgerApiErrors.RequestValidation.LedgerIdMismatch,
          Some(s"Ledger ID '$invalidLedgerId' not found."),
        )
      }
    }
  )

}
