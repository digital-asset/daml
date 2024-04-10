// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UtilsSpec extends AnyFlatSpec with Matchers {

  it should "compute largest smaller power of two" in {
    Utils.largestSmallerOrEqualPowerOfTwo(31) shouldBe 16
    Utils.largestSmallerOrEqualPowerOfTwo(16) shouldBe 16
    Utils.largestSmallerOrEqualPowerOfTwo(9) shouldBe 8
    Utils.largestSmallerOrEqualPowerOfTwo(8) shouldBe 8
    Utils.largestSmallerOrEqualPowerOfTwo(7) shouldBe 4
    Utils.largestSmallerOrEqualPowerOfTwo(5) shouldBe 4
    Utils.largestSmallerOrEqualPowerOfTwo(3) shouldBe 2
    Utils.largestSmallerOrEqualPowerOfTwo(2) shouldBe 2
    Utils.largestSmallerOrEqualPowerOfTwo(1) shouldBe 1
  }
}
