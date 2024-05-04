// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.NonNegativeNumeric
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RequireTypesTest extends AnyWordSpec with Matchers {
  "NonNegativeNumeric" should {
    "subtract" in {
      val ten = NonNegativeNumeric.tryCreate(10)
      val three = NonNegativeNumeric.tryCreate(3)
      val tenMinusThree = ten.subtract(three)
      tenMinusThree.result.value shouldBe 7
      tenMinusThree.remainder.value shouldBe 0

      val threeMinusTen = three.subtract(ten)
      threeMinusTen.result.value shouldBe 0
      threeMinusTen.remainder.value shouldBe 7
    }
  }
}
