// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test_stable.Test.Delegation._
import com.daml.ledger.test_stable.Test.DummyWithParam._
import com.daml.ledger.test_stable.Test.{Delegated, Delegation, Dummy, DummyWithParam}
import io.grpc.Status.Code

final class WronglyTypedContractId(session: LedgerSession) extends LedgerTestSuite(session) {
  test("WTExerciseFails", "Exercising on a wrong type fails", allocate(SingleParty)) {
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
  }

  test("WTFetchFails", "Fetching of the wrong type fails", allocate(TwoParties)) {
    case Participants(Participant(ledger, owner, delegate)) =>
      for {
        dummy <- ledger.create(owner, Dummy(owner))
        fakeDelegated = dummy.asInstanceOf[Primitive.ContractId[Delegated]]
        delegation <- ledger.create(owner, Delegation(owner, delegate))

        fetchFailure <- ledger
          .exercise(owner, delegation.exerciseFetchDelegated(_, fakeDelegated))
          .failed
      } yield {
        assertGrpcError(fetchFailure, Code.INVALID_ARGUMENT, "wrongly typed contract id")
      }
  }
}
