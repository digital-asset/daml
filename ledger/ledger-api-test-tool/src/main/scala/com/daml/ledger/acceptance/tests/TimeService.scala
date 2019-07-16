// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.tests

import com.daml.ledger.acceptance.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}

import scala.concurrent.Future

final class TimeService(session: LedgerSession) extends LedgerTestSuite(session) {

  private val returnCurrentTime = LedgerTest("The time service should return the current time") {
    implicit context =>
      Future {
        ???
      }
  }

  private val advanceTime = LedgerTest("The time service should correctly advance time") {
    implicit context =>
      Future {
        ???
      }
  }

  override val tests: Vector[LedgerTest] = Vector(
    returnCurrentTime,
    advanceTime
  )

}
