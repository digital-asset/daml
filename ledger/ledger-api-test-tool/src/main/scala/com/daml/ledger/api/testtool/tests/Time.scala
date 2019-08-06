// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.time.Duration

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}

import scala.concurrent.duration.DurationInt

final class Time(session: LedgerSession) extends LedgerTestSuite(session) {

  val pass =
    LedgerTest("Advancing time should return the new time") { implicit context =>
      for {
        t1 <- time()
        _ <- passTime(1.second)
        t2 <- time()
        travel = Duration.between(t1, t2)
      } yield
        assert(
          travel == Duration.ofSeconds(1),
          s"Time travel was expected to be 1 second but was instead $travel")
    }

  override val tests: Vector[LedgerTest] = Vector(pass)

}
