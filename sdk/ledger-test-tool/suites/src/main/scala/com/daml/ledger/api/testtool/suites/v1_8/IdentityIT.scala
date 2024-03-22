// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite

import scala.concurrent.Future

final class IdentityIT extends LedgerTestSuite {
  test(
    "IdNotEmpty",
    "A ledger should return a non-empty string as its identity",
    allocate(NoParties),
  ) { implicit ec =>
    { case Participants(Participant(ledger)) =>
      Future {
        assert(ledger.ledgerId.nonEmpty, "The returned ledger identifier was empty")
      }
    }
  }
}
