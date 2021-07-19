// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.model.Test.DummyWithAnnotation
import com.daml.platform.testing.WithTimeout

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Random

final class ValueLimitsIT extends LedgerTestSuite {

  test(
    "VLLargeSubmittersNumberCreateContract",
    "Create a contract with a large submitters number",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      // Need to manually allocate parties to avoid db string compression
      parties <- Future.traverse(1 to 30) { number =>
        ledger.allocateParty(
          partyIdHint =
            Some(s"deduplicationRandomParty_${number}_" + Random.alphanumeric.take(100).mkString),
          displayName = Some(s"Clone $number"),
        )
      }

      _ <- Future.traverse(1 to 500) { number =>
        ledger.submitAndWait(ledger
          .submitAndWaitRequest(
            actAs = parties.toList,
            readAs = parties.toList,
            commands = DummyWithAnnotation(parties.head, s"$number submission").create.command,
          )
        )
      }
//      request = ledger
//        .submitAndWaitRequest(
//          actAs = parties.toList,
//          readAs = parties.toList,
//          commands = DummyWithAnnotation(parties.head, "First submission").create.command,
//        )
//      _ <- ledger.submitAndWait(request)
      contracts <- ledger.activeContracts(parties.head)
      _ <- WithTimeout(5.seconds)(ledger.findCompletion(parties.head)(_.commandId == "500 submission"))
//      completions <- ledger.findCompletion(parties.head)
    } yield {
      assertLength("contract", 500, contracts)
//      assertSingleton("Single create contract expected", contracts)
      ()
    }
  })

}
