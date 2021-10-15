// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Bytes
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class OffsetBuilderSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  import OffsetBuilderSpec._

  "OffsetBuilder" should {
    "return all zeroes for the zeroth offset" in {
      val offset = OffsetBuilder.fromLong(0)

      offset should be(zeroOffset)
    }

    "always return an offset of the same length" in {
      forAll(genHighest, Gen.posNum[Int], Gen.posNum[Int]) { (highest, middle, lowest) =>
        val offset = OffsetBuilder.fromLong(highest, middle, lowest)
        offset.bytes.length should be(OffsetBuilder.end)
      }
    }

    "only change individual indexes" in {
      forAll(genHighest, Gen.posNum[Int], Gen.posNum[Int], Gen.posNum[Int]) {
        (highest, middle, lowest, newLowest) =>
          val offset = OffsetBuilder.fromLong(highest, middle, lowest)
          val modifiedOffset = OffsetBuilder.setLowestIndex(offset, newLowest)

          OffsetBuilder.split(modifiedOffset) should be((highest, middle, newLowest))
      }
    }

    "zero out the lowest index" in {
      forAll(genHighest, Gen.posNum[Int], Gen.posNum[Int]) { (highest, middle, lowest) =>
        val offset = OffsetBuilder.fromLong(highest, middle, lowest)
        val modifiedOffset = OffsetBuilder.dropLowestIndex(offset)

        OffsetBuilder.split(modifiedOffset) should be((highest, middle, 0))
      }
    }

    "retain leading zeros" in {
      val offset = OffsetBuilder.fromLong(1, 2, 3)
      val highest = offset.toByteArray.slice(OffsetBuilder.highestStart, OffsetBuilder.middleStart)
      val middle = offset.toByteArray.slice(OffsetBuilder.middleStart, OffsetBuilder.lowestStart)
      val lowest = offset.toByteArray.slice(OffsetBuilder.lowestStart, OffsetBuilder.end)

      val highestZeros = highest.dropRight(1).toSeq
      all(highestZeros) should be(0)
      highest.takeRight(1) should be(Array[Byte](1))

      val middleZeros = middle.dropRight(1).toSeq
      all(middleZeros) should be(0)
      middle.takeRight(1) should be(Array[Byte](2))

      val lowestZeros = lowest.dropRight(1).toSeq
      all(lowestZeros) should be(0)
      lowest.takeRight(1) should be(Array[Byte](3))
    }
  }
}

object OffsetBuilderSpec {
  private val zeroOffset = Offset(Bytes.fromByteArray(Array.fill(16)(0: Byte)))

  private val genHighest = Gen.chooseNum(0L, VersionedOffsetBuilder.MaxHighest)
}
