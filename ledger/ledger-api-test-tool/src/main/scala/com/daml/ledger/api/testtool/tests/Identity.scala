// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}

import scala.concurrent.Future

final class Identity(session: LedgerSession) extends LedgerTestSuite(session) {

  private[this] val identity =
    LedgerTest("IdNotEmpty", "A ledger should return a non-empty string as its identity") {
      ledger =>
        Future.successful(assert(ledger.id.nonEmpty, "The returned ledger identifier was empty"))
    }

  override val tests: Vector[LedgerTest] = Vector(identity)

}
