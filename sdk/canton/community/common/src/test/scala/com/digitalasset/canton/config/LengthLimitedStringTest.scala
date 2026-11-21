// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String3, String36}
import org.scalatest.wordspec.AnyWordSpec

class LengthLimitedStringTest extends AnyWordSpec with BaseTest {
  "LengthLimitedString" should {
    "have a correctly working .create" in {
      val ok = String255.create("123")
      val ok2 = String255.create("")
      val ok3 = String255.create("a" * 255)
      val not_ok = String255.create("a" * 256, Some("Incantation"))

      ok.value.unwrap shouldBe "123"
      ok2.value.unwrap shouldBe ""
      ok3.value.unwrap shouldBe "a" * 255
      not_ok.left.value shouldBe a[String]
      not_ok.left.value should (include("maximum length of 255") and include("Incantation"))
    }

    "have a correctly working .tryCreate" in {
      val ok = String255.tryCreate("123")
      val ok2 = String255.tryCreate("")
      val ok3 = String255.tryCreate("a" * 255)

      ok.unwrap shouldBe "123"
      ok2.unwrap shouldBe ""
      ok3.unwrap shouldBe "a" * 255
      a[IllegalArgumentException] should be thrownBy String255.tryCreate("a" * 256)
    }

    "have symmetric equality with strings" in {
      // TODO(#23301) This test does not really test symmetry and symmetry in fact does not hold.
      val s = "s"
      val s255 = String255.tryCreate("s")
      (s255 == s) shouldBe true
      (s255 == s255) shouldBe true
      // TODO(i23301): uncomment this line once fixed.
//      (s255 == "bar") shouldBe ("bar" == s255)
    }

    "respect supplementary pairs upon truncation" in {
      // String with a supplementary pair for 😂 at truncation point
      val s = "ab\uD83D\uDE02"

      val s3 = String3.createWithTruncation(s)
      s3 shouldBe "ab" // The supplementary pair is removed in full

      val s36 = String36.createWithTruncation(s)
      s36 shouldBe s
    }
  }
}
