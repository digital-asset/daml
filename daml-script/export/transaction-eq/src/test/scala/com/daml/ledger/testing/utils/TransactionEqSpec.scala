// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.testing.utils

import com.digitalasset.canton.ledger.api.refinements.ApiTypes._
import com.daml.ledger.api.v1.{value => v}

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class TransactionEqSpec extends AnyFreeSpec with Matchers {
  "Comparator" - {
    "contract id unification" in {
      val comparator = new TransactionEq.Comparator()
      comparator.unifyCid(ContractId("c1"), ContractId("c2")) shouldBe Right(())
      comparator.unifyCid(ContractId("c1"), ContractId("c2")) shouldBe Right(())
      comparator.unifyCid(ContractId("c1"), ContractId("c3")) shouldBe a[Left[_, _]]
      comparator.unifyCid(ContractId("c3"), ContractId("c2")) shouldBe a[Left[_, _]]
    }
    "party unification" in {
      val comparator = new TransactionEq.Comparator()
      comparator.unifyParty(Party("p1"), Party("p2")) shouldBe Right(())
      comparator.unifyParty(Party("p1"), Party("p2")) shouldBe Right(())
      comparator.unifyParty(Party("p1"), Party("p3")) shouldBe a[Left[_, _]]
      comparator.unifyParty(Party("p3"), Party("p2")) shouldBe a[Left[_, _]]
    }
    "value unification" in {
      val comparator = new TransactionEq.Comparator()
      comparator.unify(v.Value().withParty("p1"), v.Value().withParty("p2")) shouldBe Right(())
      comparator.unify(
        v.Value().withContractId("c1"),
        v.Value().withContractId("c2"),
      ) shouldBe Right(())
      comparator.unifyCid(ContractId("c1"), ContractId("c3")) shouldBe a[Left[_, _]]
      comparator.unifyParty(Party("p1"), Party("c3")) shouldBe a[Left[_, _]]
    }
  }
}
