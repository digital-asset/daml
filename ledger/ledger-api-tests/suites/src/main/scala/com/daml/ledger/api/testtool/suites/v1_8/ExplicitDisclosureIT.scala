// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.createdEvents
import com.daml.ledger.api.v1.commands.DisclosedContract
import com.daml.ledger.test.model.Test._

import scalaz.syntax.tag._

final class ExplicitDisclosureIT extends LedgerTestSuite {

  test(
    "EDPlaceholder",
    "Placeholder test (only to check whether it compiles)",
    allocate(Parties(1)),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val disclosedContracts = List(
      DisclosedContract(
        templateId = Some(Dummy.id.unwrap),
        contractId = "madeUpContractId",
        arguments = Some(Dummy.toNamedArguments(Dummy(party))),
        metadata = None,
      )
    )
    val request = ledger
      .submitAndWaitRequest(party, Dummy(party).create.command)
      .update(_.commands.disclosedContracts := disclosedContracts)

    for {
      response <- ledger.submitAndWaitForTransaction(request)
    } yield {
      val transaction = response.getTransaction
      val contract = assertSingleton("Expected one create event", createdEvents(transaction))
      assert(contract.metadata.isEmpty, "Not implemented yet")
    }
  })
}