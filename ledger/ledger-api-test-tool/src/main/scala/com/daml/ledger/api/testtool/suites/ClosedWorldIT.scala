// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.client.binding
import com.daml.ledger.test.semantic.SemanticTests.{Amount, Iou}
import io.grpc.Status

import java.util.regex.Pattern

class ClosedWorldIT extends LedgerTestSuite {

  private[this] val onePound = Amount(BigDecimal(1), "GBP")

  /*
   * All informees in a transaction must be allocated.
   */

  test(
    "ClosedWorldObserver",
    "Cannot execute a transaction that references unallocated observer parties",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, payer)) =>
    for {
      failure <- alpha
        .create(payer, Iou(payer, binding.Primitive.Party("unallocated"), onePound))
        .mustFail("referencing an unallocated party")
    } yield {
      assertGrpcErrorRegex(
        alpha,
        failure,
        Status.Code.INVALID_ARGUMENT,
        LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger,
        Some(Pattern.compile("Part(y|ies) not known on ledger")),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })
}
