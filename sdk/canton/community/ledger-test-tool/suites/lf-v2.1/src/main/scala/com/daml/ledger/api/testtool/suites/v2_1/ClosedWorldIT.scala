// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.java.semantic.semantictests.{Amount, Iou}
import com.digitalasset.canton.ledger.error.groups.SyncServiceRejectionErrors

import java.math.BigDecimal
import java.util.regex.Pattern

class ClosedWorldIT extends LedgerTestSuite {
  import CompanionImplicits.*

  private[this] val onePound = new Amount(BigDecimal.valueOf(1), "GBP")

  /*
   * All informees in a transaction must be allocated.
   */

  test(
    "ClosedWorldObserver",
    "Cannot execute a transaction that references unallocated observer parties",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(payer))) =>
    for {
      failure <- alpha
        .create(payer, new Iou(payer, "unallocated", onePound))
        .mustFail("referencing an unallocated party")
    } yield {
      assertGrpcErrorRegex(
        failure,
        SyncServiceRejectionErrors.PartyNotKnownOnLedger,
        Some(Pattern.compile("Part(y|ies) not known on ledger")),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })
}
