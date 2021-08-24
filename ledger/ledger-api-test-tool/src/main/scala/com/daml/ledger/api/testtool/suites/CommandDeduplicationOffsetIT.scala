// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.test.model.Test._
import com.google.rpc.code.Code

final class CommandDeduplicationOffsetIT extends LedgerTestSuite {

  test(
    "CDOffsetDeduplication",
    "Deduplicate commands based on offset",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      ledgerEnd1 <- ledger.currentEnd()
      requestA1 = ledger
        .submitRequest(party, DummyWithAnnotation(party, "First submission").create.command)
        .update(
          _.commands.deduplicationPeriod := DeduplicationPeriod.DeduplicationOffset(
            ledgerEnd1.getAbsolute
          )
        )
      _ <- ledger.submit(requestA1)
      completionsA1First <- ledger.firstCompletions(
        ledger.completionStreamRequest(ledgerEnd1)(party)
      )
      ledgerEndAfterFirstSubmit <- ledger.currentEnd()
      _ <- ledger.submit(requestA1)
      completionsA1Second <- ledger.firstCompletions(
        ledger.completionStreamRequest(ledgerEndAfterFirstSubmit)(party)
      ) // this will deduplicated as the deduplication offset includes the first submitted command
      ledgerEnd2 <- ledger.currentEnd()
      requestA2 = ledger
        .submitRequest(party, DummyWithAnnotation(party, "Second submission").create.command)
        .update(
          _.commands.deduplicationPeriod := DeduplicationPeriod
            .DeduplicationOffset(ledgerEnd2.getAbsolute),
          _.commands.commandId := requestA1.commands.get.commandId,
        )
      _ <- ledger.submit(
        requestA2
      ) // the deduplication offset is moved to the last completion so this will be successful
      completionsA2First <- ledger.firstCompletions(
        ledger.completionStreamRequest(ledgerEnd2)(party)
      )
      activeContracts <- ledger.activeContracts(party)
    } yield {
      assert(ledgerEnd1 != ledgerEnd2)
      val completionCommandId1 =
        assertSingleton("Expected only one first completion", completionsA1First)
      assert(
        completionCommandId1.commandId == requestA1.commands.get.commandId,
        "The command ID of the first completion does not match the command ID of the submission",
      )
      assert(
        completionCommandId1.status.get.code == Code.OK.value,
        s"First command did not complete OK. Had status ${completionCommandId1.status}",
      )
      val failureCommandId1 =
        assertSingleton("Expected only one first failure", completionsA1Second)
      assert(
        failureCommandId1.commandId == requestA1.commands.get.commandId,
        "The command ID of the second completion does not match the command ID of the submission",
      )
      assert(
        failureCommandId1.status.get.code == Code.ALREADY_EXISTS.value,
        s"Second completion for the first submit was not deduplicated ${failureCommandId1.status}",
      )

      val completionCommandId2 =
        assertSingleton(
          "Expected only one second successful completion",
          completionsA2First,
        )

      assert(
        completionCommandId2.commandId == requestA2.commands.get.commandId,
        "The command ID of the second completion does not match the command ID of the submission",
      )
      assert(
        completionCommandId2.status.get.code == Code.OK.value,
        s"Second command did not complete OK. Had status ${completionCommandId1.status}",
      )
      assert(
        activeContracts.size == 2,
        s"There should be 2 active contracts, but received ${activeContracts.size} contracts, with events: $activeContracts",
      )

    }
  })
}
