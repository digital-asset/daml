// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.java.model.test.DummyWithAnnotation
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta

import scala.concurrent.Future
import scala.util.Random

final class ValueLimitsIT extends LedgerTestSuite {

  test(
    "VLLargeSubmittersNumberCreateContract",
    "Create a contract with a large submitters number",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      // Need to manually allocate parties to avoid db string compression
      parties <- Future.traverse(1 to 50) { number =>
        ledger.allocateParty(
          partyIdHint =
            Some(s"deduplicationRandomParty_${number}_" + Random.alphanumeric.take(100).mkString)
        )
      }
      request = ledger
        .submitAndWaitForTransactionRequest(
          actAs = parties.toList,
          readAs = parties.toList,
          commands = new DummyWithAnnotation(
            parties.headOption.value.getValue,
            "First submission",
          ).create.commands,
          transactionShape = AcsDelta,
        )
      _ <- ledger.submitAndWaitForTransaction(request)
      contracts <- ledger.activeContracts(Some(Seq(parties.headOption.value)))
    } yield {
      assertSingleton("Single create contract expected", contracts).discard
    }
  })
}
