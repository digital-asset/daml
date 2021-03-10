// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.daml.platform.store.completions.OffsetsGenerator.genOffset

class RangeSpec extends AnyFlatSpec with Matchers {

  "Range" should "fail if and only if start offset is greater than end offset" in {
    Range(genOffset(2), genOffset(2)) // should be fine
    Range(genOffset(2), genOffset(3)) // should be fine
    assertThrows[AssertionError](Range(genOffset(3), genOffset(2)))
  }

  "lesserRangeDifference" should "return correct diff" in {
    intRange(0, 20).lesserRangeDifference(intRange(2, 10)) shouldBe Some(intRange(0, 2))
    intRange(0, 20).lesserRangeDifference(intRange(0, 10)) shouldBe None
    intRange(0, 20).lesserRangeDifference(intRange(5, 5)) shouldBe Some(intRange(0, 5))
    intRange(0, 20).lesserRangeDifference(intRange(0, 20)) shouldBe None
    intRange(5, 5).greaterRangeDifference(intRange(4, 5)) shouldBe None
  }

  "greaterRangeDifference" should "return correct diff" in {
    intRange(0, 20).greaterRangeDifference(intRange(2, 10)) shouldBe Some(intRange(10, 20))
    intRange(0, 20).greaterRangeDifference(intRange(10, 20)) shouldBe None
    intRange(0, 20).greaterRangeDifference(intRange(5, 5)) shouldBe Some(intRange(5, 20))
    intRange(0, 20).greaterRangeDifference(intRange(0, 20)) shouldBe None
    intRange(5, 5).greaterRangeDifference(intRange(5, 6)) shouldBe None
  }

  "isSubsetOf" should "return correct subset" in {
    intRange(5, 10).isSubsetOf(intRange(2, 10)) shouldBe true
    intRange(5, 10).isSubsetOf(intRange(5, 10)) shouldBe true
    intRange(5, 10).isSubsetOf(intRange(6, 10)) shouldBe false
    intRange(5, 10).isSubsetOf(intRange(5, 9)) shouldBe false
    intRange(5, 10).isSubsetOf(intRange(6, 10)) shouldBe false
  }

  "areDisjointed" should "return correct values" in {
    intRange(5, 10).areDisjointed(intRange(2, 10)) shouldBe false
    intRange(5, 10).areDisjointed(intRange(5, 10)) shouldBe false
    intRange(5, 10).areDisjointed(intRange(8, 12)) shouldBe false
    intRange(5, 10).areDisjointed(intRange(0, 5)) shouldBe true
    intRange(5, 10).areDisjointed(intRange(10, 11)) shouldBe true
    intRange(5, 10).areDisjointed(intRange(15, 21)) shouldBe true
    intRange(5, 5).areDisjointed(intRange(5, 5)) shouldBe true
  }

  "intersect" should "return correct values" in {
    intRange(5, 10).intersect(intRange(2, 10)) shouldBe Some(intRange(5, 10))
    intRange(5, 10).intersect(intRange(5, 10)) shouldBe Some(intRange(5, 10))
    intRange(5, 10).intersect(intRange(7, 8)) shouldBe Some(intRange(7, 8))
    intRange(0, 6).intersect(intRange(5, 10)) shouldBe Some(intRange(5, 6))
    intRange(5, 5).intersect(intRange(7, 8)) shouldBe None
  }

  private def intRange(startInclusive: Int, endExclusive: Int): Range =
    Range(genOffset(startInclusive), genOffset(endExclusive))
}
