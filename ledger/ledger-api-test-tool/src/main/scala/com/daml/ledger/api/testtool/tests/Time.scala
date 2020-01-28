// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.time.Duration

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}

final class Time(session: LedgerSession) extends LedgerTestSuite(session) {
  test("PassTime", "Advancing time should return the new time", allocate(NoParties)) {
    case Participants(Participant(ledger)) =>
      for {
        t1 <- ledger.time()
        _ <- ledger.passTime(Duration.ofSeconds(1))
        t2 <- ledger.time()
        travel = Duration.between(t1, t2)
      } yield assert(
        travel == Duration.ofSeconds(1),
        s"Time travel was expected to be 1 second but was instead $travel",
      )
  }
}
