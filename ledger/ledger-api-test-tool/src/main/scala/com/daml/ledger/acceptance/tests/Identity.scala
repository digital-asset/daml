// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.tests

import com.daml.ledger.acceptance.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}

final class Identity(session: LedgerSession) extends LedgerTestSuite(session) {

  private[this] val identity =
    LedgerTest("A ledger should return a non-empty string as its identity") { implicit context =>
      for (id <- ledgerId()) yield assert(id.nonEmpty, "The returned ledger identifier was empty")
    }

  override val tests: Vector[LedgerTest] = Vector(identity)

}
