// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.store

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SequencerStoreBinarySearchTest extends AnyWordSpec with Matchers {
  "SequencerStore.binarySearch" should {

    def search(array: Vector[Int], element: Int): Int =
      SequencerStore.binarySearch[Int, Int](array, identity, element)

    "return the insertion point for the needle'" in {
      // empty array
      search(Vector(), 1) shouldBe 0
      // single element
      search(Vector(1), 1) shouldBe 1
      search(Vector(1), 0) shouldBe 0
      // element smaller than the first
      search(Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), Int.MinValue) shouldBe 0
      search(Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 0) shouldBe 0
      // in case of duplicate, insert after the existing one
      search(Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 5) shouldBe 5
      search(Vector(1, 2, 3, 4, 5, 5, 7, 8, 9, 10), 5) shouldBe 6
      // non-existing element in the middle
      search(Vector(1, 2, 3, 4, 5, 7, 8, 9, 10), 6) shouldBe 5
      // element larger than the last
      search(Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 11) shouldBe 10
      search(Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), Int.MaxValue) shouldBe 10
    }
  }

}
