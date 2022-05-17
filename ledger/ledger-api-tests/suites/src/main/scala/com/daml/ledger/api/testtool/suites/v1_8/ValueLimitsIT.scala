// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.model.Test.DummyWithAnnotation

import scala.concurrent.Future
import scala.util.Random

final class ValueLimitsIT extends LedgerTestSuite {

  test(
    "VLLargeSubmittersNumberCreateContract",
    "Create a contract with a large submitters number",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      // Need to manually allocate parties to avoid db string compression
      parties <- Future.traverse(1 to 50) { number =>
        ledger.allocateParty(
          partyIdHint =
            Some(s"deduplicationRandomParty_${number}_" + Random.alphanumeric.take(100).mkString),
          displayName = Some(s"Clone $number"),
        )
      }
      request = ledger
        .submitAndWaitRequest(
          actAs = parties.toList,
          readAs = parties.toList,
          commands = DummyWithAnnotation(parties.head, "First submission").create.command,
        )
      _ <- ledger.submitAndWait(request)
      contracts <- ledger.activeContracts(parties.head)
    } yield {
      assertSingleton("Single create contract expected", contracts)
      ()
    }
  })
}
