// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.test_stable.Test.Dummy
import io.grpc.Status

class LedgerConfigurationService(session: LedgerSession) extends LedgerTestSuite(session) {
  test("ConfigSucceeds", "Return a valid configuration for a valid request", allocate(NoParties)) {
    case Participants(Participant(ledger)) =>
      for {
        config <- ledger.configuration()
      } yield {
        assert(config.minTtl.isDefined, "The minTTL field of the configuration is empty")
        assert(config.maxTtl.isDefined, "The maxTTL field of the configuration is empty")
      }
  }

  test("ConfigLedgerId", "Return NOT_FOUND to invalid ledger identifier", allocate(NoParties)) {
    case Participants(Participant(ledger)) =>
      val invalidLedgerId = "THIS_IS_AN_INVALID_LEDGER_ID"
      for {
        failure <- ledger.configuration(overrideLedgerId = Some(invalidLedgerId)).failed
      } yield {
        assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
      }
  }

  test(
    "CSLSuccessIfLetRight",
    "Submission returns OK if LET is within the accepted interval",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      // The maximum accepted clock skew depends on the ledger and is not exposed through the LedgerConfigurationService,
      // and there might be an actual clock skew between the devices running the test and the ledger.
      // This test therefore does not attempt to simulate any clock skew
      // but simply checks whether basic command submission with an unmodified LET works.
      val request = ledger.submitRequest(party, Dummy(party).create.command)
      for {
        _ <- ledger.submit(request)
      } yield {
        // No assertions to make, since the command went through as expected
      }
  }
}
