// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UtilsSpec extends AnyFlatSpec with Matchers {

  it should "compute largest smaller power of two" in {
    Utils.largestSmallerPowerOfTwo(31) shouldBe 16
    Utils.largestSmallerPowerOfTwo(16) shouldBe 16
    Utils.largestSmallerPowerOfTwo(9) shouldBe 8
    Utils.largestSmallerPowerOfTwo(8) shouldBe 8
    Utils.largestSmallerPowerOfTwo(7) shouldBe 4
    Utils.largestSmallerPowerOfTwo(5) shouldBe 4
    Utils.largestSmallerPowerOfTwo(3) shouldBe 2
    Utils.largestSmallerPowerOfTwo(2) shouldBe 2
    Utils.largestSmallerPowerOfTwo(1) shouldBe 1
  }
}
