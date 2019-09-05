// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.test_stable.Test.Dummy

final class MultiNode(session: LedgerSession) extends LedgerTestSuite(session) {

  val authorization =
    LedgerTest(
      "MultiNodeAuthorization",
      "Should not be able to issue commands on a node on which the party is not hosted") {
      context =>
        for {
          Vector(alpha, beta) <- context.participants(2)
          _ <- skipIf("This test requires a multi-node setup to be run")(alpha eq beta)
          alice <- alpha.allocateParty()
          _ <- beta.create(alice, Dummy(alice)).failed
        } yield {}
    }

  override val tests: Vector[LedgerTest] = Vector(authorization)

}
