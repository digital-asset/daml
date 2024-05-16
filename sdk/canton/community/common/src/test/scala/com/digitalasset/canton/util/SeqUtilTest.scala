// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class SeqUtilTest extends AnyWordSpec with BaseTest {

  "randomSubsetShuffle" should {
    "pick a random subset of the given size" in {
      val iterations = 1000
      for { i <- 1 to iterations } {
        val subset = SeqUtil.randomSubsetShuffle(1 to 100, 30, new Random(i))
        subset should have size 30
        subset.distinct shouldBe subset
        (1 to 1000) should contain allElementsOf subset
      }
    }

    "deal with tiny subsets" in {
      val random = new Random(123)
      val subsetSize1 = SeqUtil.randomSubsetShuffle(1 to 1000, 1, random)
      subsetSize1 should have size 1

      val subsetSize0 = SeqUtil.randomSubsetShuffle(1 to 1000, 0, random)
      subsetSize0 should have size 0
    }

    "deal with large subsets" in {
      val random = new Random(345)
      val subsetSize999 = SeqUtil.randomSubsetShuffle(1 to 1000, 999, random)
      subsetSize999 should have size 999
      subsetSize999.distinct shouldBe subsetSize999
      (1 to 1000) should contain allElementsOf subsetSize999

      val fullShuffle = SeqUtil.randomSubsetShuffle(1 to 1000, 1000, random)
      fullShuffle.sorted shouldBe (1 to 1000)
    }

    "cap the size" in {
      val random = new Random(678)
      val more = SeqUtil.randomSubsetShuffle(1 to 10, 20, random)
      more.sorted shouldBe (1 to 10)
    }
  }
}
