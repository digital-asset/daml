// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.offset.Offset
import com.daml.lf.data
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class OffsetBuilderSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "OffsetBuilder" should {
    val zeroBytes = data.Bytes.fromByteArray(Array.fill(16)(0: Byte))

    def triple(offset: Offset): (Long, Long, Long) =
      (
        OffsetBuilder.highestIndex(offset),
        OffsetBuilder.middleIndex(offset),
        OffsetBuilder.lowestIndex(offset),
      )

    "set 0 bytes" in {
      OffsetBuilder.fromLong(0).bytes shouldEqual zeroBytes
    }

    "extract the correct indexes" in {
      val offset = OffsetBuilder.fromLong(1, 2, 3)
      triple(offset) shouldBe ((1, 2, 3))
    }

    "only change individual indexes" in {
      val offset = OffsetBuilder.fromLong(1, 2, 3)

      triple(OffsetBuilder.setLowestIndex(offset, 17)) shouldBe ((1, 2, 17))
      triple(OffsetBuilder.setMiddleIndex(offset, 17)) shouldBe ((1, 17, 3))
    }

    "zero out the middle and lowest index" in {
      val offset = OffsetBuilder.fromLong(1, 2, 3)
      triple(OffsetBuilder.onlyKeepHighestIndex(offset)) shouldBe ((1, 0, 0))
    }

    "zero out the lowest index" in {
      val offset = OffsetBuilder.fromLong(1, 2, 3)
      triple(OffsetBuilder.dropLowestIndex(offset)) shouldBe ((1, 2, 0))
    }

    "retain leading zeros" in {
      val offset = OffsetBuilder.fromLong(1, 2, 3)
      val highest = offset.toByteArray.slice(OffsetBuilder.highestStart, OffsetBuilder.middleStart)
      val middle = offset.toByteArray.slice(OffsetBuilder.middleStart, OffsetBuilder.lowestStart)
      val lowest = offset.toByteArray.slice(OffsetBuilder.lowestStart, OffsetBuilder.end)

      val highestZeros = highest.dropRight(1)
      highestZeros.forall(_ == 0) shouldBe true
      highest.takeRight(1)(0) shouldBe 1

      val middleZeros = middle.dropRight(1)
      middleZeros.forall(_ == 0) shouldBe true
      middle.takeRight(1)(0) shouldBe 2

      val lowestZeros = lowest.dropRight(1)
      lowestZeros.forall(_ == 0) shouldBe true
      lowest.takeRight(1)(0) shouldBe 3
    }

    "convert back and forth" in {
      forAll { (highest: Long, middle: Int, lowest: Int) =>
        whenever(highest >= 0 && middle >= 0 && lowest >= 0) {
          val offset = OffsetBuilder.fromLong(highest, middle, lowest)
          OffsetBuilder.highestIndex(offset) should be(highest)
          OffsetBuilder.middleIndex(offset) should be(middle)
          OffsetBuilder.lowestIndex(offset) should be(lowest)
        }
      }
    }

    "always has the same length" in {
      forAll { (highest: Long, middle: Int, lowest: Int) =>
        whenever(highest >= 0 && middle >= 0 && lowest >= 0) {
          val offset = OffsetBuilder.fromLong(highest, middle, lowest)
          offset.bytes.length should be(OffsetBuilder.end)
        }
      }
    }
  }
}
