// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.client.binding.Primitive
import com.digitalasset.ledger.test_stable.Test.DummyWithParam._
import com.digitalasset.ledger.test_stable.Test.Delegation._
import com.digitalasset.ledger.test_stable.Test.{Delegated, Delegation, Dummy, DummyWithParam}
import io.grpc.Status.Code

final class WronglyTypedContractId(session: LedgerSession) extends LedgerTestSuite(session) {

  private[this] val failedExercise =
    LedgerTest("WTExerciseFails", "Exercising on a wrong type fails") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()

        dummy <- ledger.create(party, Dummy(party))
        fakeDummyWithParam = dummy.asInstanceOf[Primitive.ContractId[DummyWithParam]]
        exerciseFailure <- ledger
          .exercise(party, fakeDummyWithParam.exerciseDummyChoice2(_, "txt"))
          .failed
      } yield {
        assertGrpcError(exerciseFailure, Code.INVALID_ARGUMENT, "wrongly typed contract id")
      }
    }

  private[this] val failedFetch =
    LedgerTest("WTFetchFails", "Fetching of the wrong type fails") { context =>
      for {
        ledger <- context.participant()
        Vector(owner, delegate) <- ledger.allocateParties(2)

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
  override val tests: Vector[LedgerTest] = Vector(failedExercise, failedFetch)
}
