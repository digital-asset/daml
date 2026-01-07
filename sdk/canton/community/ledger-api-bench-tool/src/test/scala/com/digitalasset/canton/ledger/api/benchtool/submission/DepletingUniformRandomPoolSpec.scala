// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DepletingUniformRandomPoolSpec extends AnyFlatSpec with Matchers {

  it should "put and pop from a pool" in {
    val tested = new DepletingUniformRandomPool[Int](RandomnessProvider.forSeed(0))
    intercept[NoSuchElementException](tested.pop())
    tested.put(1)
    tested.pop() shouldBe 1
    tested.put(1)
    tested.put(2)
    tested.put(3)
    tested.pop() shouldBe 3
    tested.pop() shouldBe 1
    tested.pop() shouldBe 2
    intercept[NoSuchElementException](tested.pop())
  }
}
