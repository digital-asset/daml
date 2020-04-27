// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  SingleParty,
  allocate
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.assertGrpcError
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.daml.ledger.client.binding
import com.daml.ledger.test.SemanticTests.{Amount, Iou}
import io.grpc.Status

class ClosedWorld(session: LedgerSession) extends LedgerTestSuite(session) {

  private[this] val onePound = Amount(BigDecimal(1), "GBP")

  /*
   * All informees in a transaction must be allocated.
   */

  test(
    "ClosedWorldObserver",
    "Cannot execute a transaction that references unallocated observer parties",
    allocate(SingleParty),
  ) {
    case Participants(Participant(alpha, payer)) =>
      for {
        failure <- alpha
          .create(payer, Iou(payer, binding.Primitive.Party("unallocated"), onePound))
          .failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Party not known on ledger")
      }
  }
}
