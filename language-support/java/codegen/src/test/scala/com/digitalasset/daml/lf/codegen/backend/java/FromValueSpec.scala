// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ut.bar.{Bar, Haha, ParameterizedContractId}
import com.daml.ledger.javaapi.data.codegen.ContractId
import shapeless.test.illTyped

import scala.annotation.nowarn

final class FromValueSpec extends AnyWordSpec with Matchers {

  "contractId<Bar>" should {
    "not be cast to Bar.ContractId" in {
      val fromConstructor: ParameterizedContractId[Bar] =
        new ParameterizedContractId(new Bar.ContractId("SomeID"))
      val parametrizedContractId: ParameterizedContractId[Bar] =
        ParameterizedContractId
          .valueDecoder(Bar.valueDecoder())
          .decode(fromConstructor.toValue(_.toValue))
      val contractIdBar: ContractId[Bar] = parametrizedContractId.parameterizedContractId

      illTyped(
        "contractIdBar: Bar.ContractId",
        "type mismatch.+ContractId\\[.+Bar\\].+Bar.ContractId",
      )

      contractIdBar shouldBe a[Bar.ContractId]
      Bar.ContractId.fromContractId(contractIdBar) shouldBe a[Bar.ContractId]
    }

    "pass down indirectly from templates" in {
      val haha =
        new Haha(new ParameterizedContractId(new Bar.ContractId("SomeID")), "nonsenseparty")
      val roundtrip = Haha.valueDecoder().decode(haha.toValue)
      roundtrip.p.parameterizedContractId shouldBe a[Bar.ContractId]
    }

    "decode with deprecated fromValue Method" in {
      val fromConstructor: ParameterizedContractId[Bar] =
        new ParameterizedContractId(new Bar.ContractId("SomeID"))
      @nowarn("msg=method fromValue in class .* is deprecated")
      val parametrizedContractId: ParameterizedContractId[Bar] =
        ParameterizedContractId
          .fromValue(fromConstructor.toValue(_.toValue), Bar.valueDecoder().decode)
      val contractIdBar: ContractId[Bar] = parametrizedContractId.parameterizedContractId
      contractIdBar should not be a[Bar.ContractId]
    }
  }
}
