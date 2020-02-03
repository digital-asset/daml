// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}

import scala.concurrent.Future

final class Identity(session: LedgerSession) extends LedgerTestSuite(session) {
  test(
    "IdNotEmpty",
    "A ledger should return a non-empty string as its identity",
    allocate(NoParties),
  ) {
    case Participants(Participant(ledger)) =>
      Future {
        assert(ledger.ledgerId.nonEmpty, "The returned ledger identifier was empty")
      }
  }
}
