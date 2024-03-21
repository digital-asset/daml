// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.{ScalaCheckDrivenPropertyChecks}

class ProfilerUnmangleTest extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  "unmangleLenient" should {
    "be identity on strings without $" in {
      forAll(minSuccessful(1000)) { (s: String) =>
        whenever(!s.contains('$')) {
          Profile.unmangleLenient(s) shouldBe (s)
        }
      }
    }
    "unmangle 4-digit escape" in {
      Profile.unmangleLenient("$u0027") shouldBe ("'")
    }
    "unmangle 8-digit escape" in {
      Profile.unmangleLenient("$U0001F937") shouldBe ("🤷")
    }
    "unmangle escaped $" in {
      Profile.unmangleLenient("$$") shouldBe ("$")
    }
    "be lenient on invalid escapes" in {
      Profile.unmangleLenient("$") shouldBe ("$")
      Profile.unmangleLenient("$u") shouldBe ("$u")
      Profile.unmangleLenient("$U") shouldBe ("$U")
      Profile.unmangleLenient("$u1") shouldBe ("$u1")
      Profile.unmangleLenient("$u12") shouldBe ("$u12")
      Profile.unmangleLenient("$u123") shouldBe ("$u123")
      Profile.unmangleLenient("$uxyz1") shouldBe ("$uxyz1")
    }
  }
}
