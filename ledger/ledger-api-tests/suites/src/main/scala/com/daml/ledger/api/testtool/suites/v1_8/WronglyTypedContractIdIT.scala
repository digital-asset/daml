// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test.Delegation._
import com.daml.ledger.test.model.Test.DummyWithParam._
import com.daml.ledger.test.model.Test.{Delegated, Delegation, Dummy, DummyWithParam}

final class WronglyTypedContractIdIT extends LedgerTestSuite {
  test("WTExerciseFails", "Exercising on a wrong type fails", allocate(SingleParty))(
    implicit ec => { case Participants(Participant(ledger, party)) =>
      for {
        dummy <- ledger.create(party, Dummy(party))
        fakeDummyWithParam = dummy.asInstanceOf[Primitive.ContractId[DummyWithParam]]
        exerciseFailure <- ledger
          .exercise(party, fakeDummyWithParam.exerciseDummyChoice2(_, "txt"))
          .mustFail("exercising on a wrong type")
      } yield {
        assertGrpcError(
          exerciseFailure,
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError,
          Some("wrongly typed contract id"),
          checkDefiniteAnswerMetadata = true,
        )
      }
    }
  )

  test("WTFetchFails", "Fetching of the wrong type fails", allocate(SingleParty))(implicit ec => {
    case Participants(Participant(ledger, party)) =>
      for {
        dummy <- ledger.create(party, Dummy(party))
        fakeDelegated = dummy.asInstanceOf[Primitive.ContractId[Delegated]]
        delegation <- ledger.create(party, Delegation(party, party))

        fetchFailure <- ledger
          .exercise(party, delegation.exerciseFetchDelegated(_, fakeDelegated))
          .mustFail("fetching the wrong type")
      } yield {
        assertGrpcError(
          fetchFailure,
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError,
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
      dummy <- ledger.create(party, Dummy(party))
      fakeDummyWithParam = dummy.asInstanceOf[Primitive.ContractId[DummyWithParam]]
      failure <- ledger
        .submitAndWait(
          ledger.submitAndWaitRequest(
            party,
            dummy.exerciseClone(party).command,
            fakeDummyWithParam.exerciseDummyChoice2(party, "").command,
          )
        )
        .mustFail("exercising on a wrong type")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError,
        Some("wrongly typed contract id"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })
}
