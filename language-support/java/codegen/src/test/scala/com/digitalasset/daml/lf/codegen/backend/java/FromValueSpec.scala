// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ut.bar.{Bar, ParameterizedContractId}
import com.daml.ledger.javaapi.data.codegen.ContractId
import shapeless.test.illTyped

final class FromValueSpec extends AnyWordSpec with Matchers {

  "contractId<Bar>" should {
    "not be cast to Bar.ContractId" in {
      val fromConstructor: ParameterizedContractId[Bar] =
        new ParameterizedContractId(new Bar.ContractId("SomeID"))
      val parametrizedContractId: ParameterizedContractId[Bar] =
        ParameterizedContractId.fromValue(fromConstructor.toValue(_.toValue), Bar.fromValue)
      val contractIdBar: ContractId[Bar] = parametrizedContractId.parameterizedContractId

      illTyped(
        "contractIdBar: Bar.ContractId",
        "type mismatch.+ContractId\\[.+Bar\\].+Bar.ContractId",
      )

      contractIdBar should not be a[Bar.ContractId]
      Bar.ContractId.fromContractId(contractIdBar) shouldBe a[Bar.ContractId]
    }
  }
}
