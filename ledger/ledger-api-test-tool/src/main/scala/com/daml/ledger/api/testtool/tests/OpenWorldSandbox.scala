// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  SingleParty,
  allocate
}
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.client.binding
import com.digitalasset.ledger.test.SemanticTests.{Amount, Iou}

class OpenWorldSandbox(session: LedgerSession) extends LedgerTestSuite(session) {

  private[this] val onePound = Amount(BigDecimal(1), "GBP")

  /*
   * All party values in a transaction must be allocated.
   */

  test(
    "PartiesWillBeImplicitlyAllocated",
    "A transaction that references unallocated informee parties will go through on the sandbox",
    allocate(SingleParty),
  ) {
    case Participants(Participant(alpha, payer)) =>
      for {
        _ <- alpha
          .create(payer, Iou(payer, binding.Primitive.Party("unallocated"), onePound))
      } yield ()
  }
}
