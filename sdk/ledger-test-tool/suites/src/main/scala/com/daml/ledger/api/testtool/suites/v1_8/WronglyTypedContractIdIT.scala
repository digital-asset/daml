// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.java.model.test.{Delegated, Delegation, Dummy, DummyWithParam}

import scala.jdk.CollectionConverters._

final class WronglyTypedContractIdIT extends LedgerTestSuite {
  import CompanionImplicits._

  test("WTExerciseFails", "Exercising on a wrong type fails", allocate(SingleParty))(
    implicit ec => { case Participants(Participant(ledger, party)) =>
      for {
        dummy <- ledger.create(party, new Dummy(party))
        fakeDummyWithParam = new DummyWithParam.ContractId(dummy.contractId)
        exerciseFailure <- ledger
          .exercise(party, fakeDummyWithParam.exerciseDummyChoice2("txt"))
          .mustFail("exercising on a wrong type")
      } yield {
        assertGrpcError(
          exerciseFailure,
          LedgerApiErrors.CommandExecution.Interpreter.WronglyTypedContract,
          Some("wrongly typed contract id"),
          checkDefiniteAnswerMetadata = true,
        )
      }
    }
  )

  test("WTFetchFails", "Fetching of the wrong type fails", allocate(SingleParty))(implicit ec => {
    case Participants(Participant(ledger, party)) =>
      for {
        dummy <- ledger.create(party, new Dummy(party))
        fakeDelegated = new Delegated.ContractId(dummy.contractId)
        delegation: Delegation.ContractId <- ledger.create(party, new Delegation(party, party))

        fetchFailure <- ledger
          .exercise(party, delegation.exerciseFetchDelegated(fakeDelegated))
          .mustFail("fetching the wrong type")
      } yield {
        assertGrpcError(
          fetchFailure,
          LedgerApiErrors.CommandExecution.Interpreter.WronglyTypedContract,
          Some("wrongly typed contract id"),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  test(
    "WTMultipleExerciseFails",
    "Exercising on a wrong type fails after correct exercise in same transaction",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
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
        LedgerApiErrors.CommandExecution.Interpreter.WronglyTypedContract,
        Some("wrongly typed contract id"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })
}
