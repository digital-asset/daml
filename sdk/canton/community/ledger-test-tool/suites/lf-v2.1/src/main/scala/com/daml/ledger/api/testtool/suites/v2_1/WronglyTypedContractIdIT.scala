// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.java.model.test.{Dummy, DummyWithParam}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors

import scala.jdk.CollectionConverters.*

final class WronglyTypedContractIdIT extends LedgerTestSuite {
  import CompanionImplicits.*

  test("WTExerciseFails", "Exercising on a wrong type fails", allocate(SingleParty))(
    implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
      for {
        dummy <- ledger.create(party, new Dummy(party))
        fakeDummyWithParam = new DummyWithParam.ContractId(dummy.contractId)
        exerciseFailure <- ledger
          .exercise(party, fakeDummyWithParam.exerciseDummyChoice2("txt"))
          .mustFail("exercising on a wrong type")
      } yield {
        assertGrpcError(
          exerciseFailure,
          CommandExecutionErrors.Interpreter.WronglyTypedContract,
          Some("wrongly typed contract id"),
          checkDefiniteAnswerMetadata = true,
        )
      }
    }
  )

  test(
    "WTMultipleExerciseFails",
    "Exercising on a wrong type fails after correct exercise in same transaction",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummy: Dummy.ContractId <- ledger.create(party, new Dummy(party))
      fakeDummyWithParam = new DummyWithParam.ContractId(dummy.contractId)
      failure <- ledger
        .submitAndWait(
          ledger.submitAndWaitRequest(
            party,
            (dummy
              .exerciseClone()
              .commands
              .asScala ++ fakeDummyWithParam.exerciseDummyChoice2("").commands.asScala).asJava,
          )
        )
        .mustFail("exercising on a wrong type")
    } yield {
      assertGrpcError(
        failure,
        CommandExecutionErrors.Interpreter.WronglyTypedContract,
        Some("wrongly typed contract id"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })
}
