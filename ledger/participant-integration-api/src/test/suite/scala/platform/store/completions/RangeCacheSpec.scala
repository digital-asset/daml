// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.daml.platform.store.completions.RangeGenerator.genOffset
import com.daml.ledger.participant.state.v1.Offset

import scala.collection.SortedMap

class RangeCacheSpec extends AnyFlatSpec with Matchers {

  private val emptyMap: SortedMap[Offset, Int] = SortedMap.empty[Offset, Int]

  "RangeCache" should "limit maximum stored elements to last maxItems" in {
    RangeCache(intRange(0, 4), 5, genMap(1, 4)).cache.size shouldBe 3
    RangeCache(intRange(0, 10), 5, genMap(1, 10)).cache.size shouldBe 5
  }

  it should "not allow for cache outside the given range" in {
    assertThrows[AssertionError](RangeCache(intRange(0, 3), 10, genMap(2, 5)))
    assertThrows[AssertionError](RangeCache(intRange(3, 6), 10, genMap(0, 5)))
  }

  "appending" should "limit maximum stored elements to last maxItems" in {
    RangeCache(intRange(0, 1), 5, emptyMap)
      .append(intRange(0, 10), genMap(1, 4))
      .cache
      .size shouldBe 3
    RangeCache(intRange(0, 1), 5, emptyMap)
      .append(intRange(0, 10), genMap(1, 10))
      .cache
      .size shouldBe 5
  }

  it should "not allow for cache outside the given range" in {
    val emptyCache1 = RangeCache(intRange(0, 3), 10, emptyMap)
    assertThrows[AssertionError](emptyCache1.append(intRange(0, 5), genMap(1, 7)))
    val emptyCache2 = RangeCache(intRange(3, 6), 10, emptyMap)
    assertThrows[AssertionError](emptyCache2.append(intRange(3, 7), genMap(1, 7)))
  }

  it should "append values correctly" in {
    RangeCache(intRange(0, 3), 10, genMap(1, 4))
      .append(intRange(3, 5), genMap(4, 6)) shouldBe RangeCache(intRange(0, 5), 10, genMap(1, 6))

    RangeCache(intRange(3, 6), 10, genMap(4, 7))
      .append(intRange(0, 3), genMap(1, 4)) shouldBe RangeCache(intRange(0, 6), 10, genMap(1, 7))
  }

  "slice" should "return correct slice" in {
    RangeCache(intRange(0, 10), 5, genMap(1, 5)).slice(intRange(0, 3)) shouldBe Some(
      RangeCache(intRange(0, 3), 5, genMap(1, 4))
    )
    RangeCache(intRange(0, 10), 10, genMap(1, 11)).slice(intRange(3, 6)) shouldBe Some(
      RangeCache(intRange(3, 6), 10, genMap(4, 7))
    )
    RangeCache(intRange(0, 10), 10, genMap(1, 11)).slice(intRange(11, 12)) shouldBe None
  }

  private def intRange(startInclusive: Int, endExclusive: Int): Range =
    Range(genOffset(startInclusive), genOffset(endExclusive))

  private def genMap(from: Int, untilVal: Int): SortedMap[Offset, Int] = SortedMap(
    (from until untilVal).map(i => genOffset(i) -> i): _*
  )
}
