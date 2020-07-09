// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data
import org.scalatest.{Matchers, WordSpec}

class NumericOffsetSpec extends WordSpec with Matchers {

  "KVOffset" should {
    val zeroBytes = data.Bytes.fromByteArray(Array.fill(16)(0: Byte))

    def triple(offset: Offset) =
      (
        NumericOffset.highestIndex(offset),
        NumericOffset.middleIndex(offset),
        NumericOffset.lowestIndex(offset))

    "set 0 bytes" in {
      NumericOffset.fromLong(0).bytes shouldEqual zeroBytes
    }

    "extract the correct indexes" in {
      val offset = NumericOffset.fromLong(1, 2, 3)
      triple(offset) shouldBe ((1, 2, 3))
    }

    "only change individual indexes" in {
      val offset = NumericOffset.fromLong(1, 2, 3)

      triple(NumericOffset.setLowestIndex(offset, 17)) shouldBe ((1, 2, 17))
      triple(NumericOffset.setMiddleIndex(offset, 17)) shouldBe ((1, 17, 3))
    }

    "zero out the middle and lowest index" in {
      val offset = NumericOffset.fromLong(1, 2, 3)
      triple(NumericOffset.onlyKeepHighestIndex(offset)) shouldBe ((1, 0, 0))
    }

    "retain leading zeros" in {
      val offset = NumericOffset.fromLong(1, 2, 3)
      val highest = offset.toByteArray.slice(NumericOffset.highestStart, NumericOffset.middleStart)
      val middle = offset.toByteArray.slice(NumericOffset.middleStart, NumericOffset.lowestStart)
      val lowest = offset.toByteArray.slice(NumericOffset.lowestStart, NumericOffset.end)

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
  }
}
