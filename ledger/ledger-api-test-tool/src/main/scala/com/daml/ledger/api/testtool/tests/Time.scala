// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.time.Duration

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}

import scala.concurrent.duration.DurationInt

final class Time(session: LedgerSession) extends LedgerTestSuite(session) {

  val pass =
    LedgerTest("PassTime", "Advancing time should return the new time") { context =>
      for {
        ledger <- context.participant()
        t1 <- ledger.time()
        _ <- ledger.passTime(1.second)
        t2 <- ledger.time()
        travel = Duration.between(t1, t2)
      } yield
        assert(
          travel == Duration.ofSeconds(1),
          s"Time travel was expected to be 1 second but was instead $travel")
    }

  override val tests: Vector[LedgerTest] = Vector(pass)

}
