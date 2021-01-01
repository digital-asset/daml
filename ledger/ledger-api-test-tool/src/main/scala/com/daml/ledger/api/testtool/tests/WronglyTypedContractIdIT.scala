// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test.Delegation._
import com.daml.ledger.test.model.Test.DummyWithParam._
import com.daml.ledger.test.model.Test.{Delegated, Delegation, Dummy, DummyWithParam}
import io.grpc.Status.Code

final class WronglyTypedContractIdIT extends LedgerTestSuite {
  test("WTExerciseFails", "Exercising on a wrong type fails", allocate(SingleParty))(
    implicit ec => {
      case Participants(Participant(ledger, party)) =>
        for {
          dummy <- ledger.create(party, Dummy(party))
          fakeDummyWithParam = dummy.asInstanceOf[Primitive.ContractId[DummyWithParam]]
          exerciseFailure <- ledger
            .exercise(party, fakeDummyWithParam.exerciseDummyChoice2(_, "txt"))
            .failed
        } yield {
          assertGrpcError(exerciseFailure, Code.INVALID_ARGUMENT, "wrongly typed contract id")
        }
    })

  test("WTFetchFails", "Fetching of the wrong type fails", allocate(SingleParty))(implicit ec => {
    case Participants(Participant(ledger, party)) =>
      for {
        dummy <- ledger.create(party, Dummy(party))
        fakeDelegated = dummy.asInstanceOf[Primitive.ContractId[Delegated]]
        delegation <- ledger.create(party, Delegation(party, party))

        fetchFailure <- ledger
          .exercise(party, delegation.exerciseFetchDelegated(_, fakeDelegated))
          .failed
      } yield {
        assertGrpcError(fetchFailure, Code.INVALID_ARGUMENT, "wrongly typed contract id")
      }
  })
}
