// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test._

class TransactionServiceOutputsIT extends LedgerTestSuite {
  test(
    "TXUnitAsArgumentToNothing",
    "Daml engine returns Unit as argument to Nothing",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val template = NothingArgument(party, Primitive.Optional.empty)
    val create = ledger.submitAndWaitRequest(party, template.create.command)
    for {
      transactionResponse <- ledger.submitAndWaitForTransaction(create)
    } yield {
      val contract = assertSingleton(
        "UnitAsArgumentToNothing",
        createdEvents(transactionResponse.getTransaction),
      )
      assertEquals("UnitAsArgumentToNothing", contract.getCreateArguments, template.arguments)
    }
  })

  test(
    "TXAgreementTextExplicit",
    "Expose the agreement text for templates with an explicit agreement text",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, Dummy(party))
      transactions <- ledger.flatTransactionsByTemplateId(Dummy.id, party)
    } yield {
      val contract = assertSingleton("AgreementText", transactions.flatMap(createdEvents))
      assertEquals("AgreementText", contract.getAgreementText, s"'$party' operates a dummy.")
    }
  })

  test(
    "TXAgreementTextDefault",
    "Expose the default text for templates without an agreement text",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, DummyWithParam(party))
      transactions <- ledger.flatTransactions(party)
    } yield {
      val contract = assertSingleton("AgreementTextDefault", transactions.flatMap(createdEvents))
      assertEquals("AgreementTextDefault", contract.getAgreementText, "")
    }
  })
}
