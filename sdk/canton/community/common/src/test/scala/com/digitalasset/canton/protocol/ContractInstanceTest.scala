// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.serialization.HasCryptographicEvidenceTest
import org.scalatest.wordspec.AnyWordSpec

class ContractInstanceTest extends AnyWordSpec with HasCryptographicEvidenceTest with BaseTest {

  "ContractInstance" should {

    "Encode/decode" in {
      val expected = ExampleContractFactory.build()
      val actual = ContractInstance.decode(expected.encoded).value
      actual shouldBe expected
    }

    "Fail with deep value" in {
      val example = ExampleContractFactory.build().inst: LfFatContractInst
      val inst = LfFatContractInst.fromCreateNode(
        example.toCreateNode.copy(arg = ExampleTransactionFactory.veryDeepValue),
        example.createdAt,
        example.authenticationData,
      )
      val err = ContractInstance.create(inst).left.value
      err should fullyMatch regex "Failed to encode contract instance:.*Provided Daml-LF value to encode exceeds maximum nesting level of 100.*"
    }
  }
}
