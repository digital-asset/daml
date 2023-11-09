// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import java.math.BigInteger

import com.daml.ledger.api.testtool.infrastructure.HexOffset
import com.daml.lf.data.Ref
import org.scalacheck.Gen
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.{Checkers, ScalaCheckPropertyChecks}

class HexOffsetSpec
    extends AnyWordSpec
    with Matchers
    with Checkers
    with ScalaCheckPropertyChecks
    with OptionValues {

  private val offsets: Gen[Ref.HexString] =
    Gen.hexStr
      .suchThat(_.nonEmpty)
      .suchThat(_.length % 2 == 0)
      .map(_.toLowerCase)
      .map(Ref.HexString.assertFromString)

  "building lexicographical string" should {

    "return None if the lexicographically previous string does not exist" in {
      HexOffset.previous(
        Ref.HexString.assertFromString("000000000000")
      ) shouldBe None
    }

    "return expected lower offsets" in {
      forAll(
        Table(
          "offset" -> "expected offset",
          "00000001" -> "00000000",
          "0000000a" -> "00000009",
          "00000100" -> "000000ff",
          "a0000000" -> "9fffffff",
          "00000ab0" -> "00000aaf",
          "00007a90" -> "00007a8f",
          "00007a94" -> "00007a93",
        )
      ) { case (offset, expectedOffset) =>
        HexOffset.previous(
          Ref.HexString.assertFromString(offset)
        ) shouldBe Some(Ref.HexString.assertFromString(expectedOffset))
      }
    }

    "return lower offset" in forAll(offsets) { offset =>
      if (new BigInteger(offset, 16) == BigInteger.ZERO) {
        HexOffset.previous(offset) shouldBe None
      } else {
        val lowerOffset = HexOffset.previous(offset).value
        offset should be > [String] lowerOffset
      }
    }

  }
}
