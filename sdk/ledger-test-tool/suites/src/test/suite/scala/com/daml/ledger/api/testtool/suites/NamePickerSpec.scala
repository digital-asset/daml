// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.NamePicker
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class NamePickerSpec extends AnyWordSpec with Matchers {

  "NamePicker" should {
    "not have duplicates in the canonical alphabet" in {
      val myPicker = NamePicker("aba")
      myPicker.canon shouldBe "ab"
    }

    "have min and max" in {
      val myPicker = NamePicker("01ab")
      myPicker.mx shouldBe 'b'
      myPicker.mn shouldBe '0'
    }

    "check for membership" in {
      val myPicker = NamePicker("01ab")
      myPicker.belongs("ab01") shouldBe true
      myPicker.belongs("ab02") shouldBe false
      myPicker.belongs("") shouldBe true
    }

    "recognize all lowest" in {
      val myPicker = NamePicker("01ab")
      myPicker.allLowest("CCC") shouldBe false
      myPicker.allLowest("aaa") shouldBe false
      myPicker.allLowest("0a") shouldBe false
      myPicker.allLowest("a0") shouldBe false
      myPicker.allLowest("00") shouldBe true
      myPicker.allLowest("0") shouldBe true
      myPicker.allLowest("") shouldBe true
    }

    "lower the input char" in {
      val myPicker = NamePicker("01ab")
      myPicker.lower('c') shouldBe None
      myPicker.lower('b') shouldBe Some('a')
      myPicker.lower('0') shouldBe None
    }

    "lower the input string" in {
      val myPicker = NamePicker("01ab")
      myPicker.lower("abc") shouldBe None
      myPicker.lower("00") shouldBe Some("0")
      myPicker.lower("0") shouldBe Some("")
      myPicker.lower("") shouldBe None

      myPicker.lower("ab") shouldBe Some("aa")
      myPicker.lower("a0") shouldBe Some("1b")
    }

    "lower the input string constrained" in {
      val myPicker = NamePicker("01ab")
      myPicker.lowerConstrained("abc", "aaa") shouldBe None
      myPicker.lowerConstrained("aba", "aac") shouldBe None

      myPicker.lowerConstrained("aba", "abb") shouldBe None
      myPicker.lowerConstrained("000", "00") shouldBe None
      myPicker.lowerConstrained("0", "") shouldBe None

      myPicker.lowerConstrained("aa0", "aa") shouldBe None
      myPicker.lowerConstrained("ab0", "aa") shouldBe Some("aab")

      myPicker.lowerConstrained("aba", "aaa") shouldBe Some("ab1")
      myPicker.lowerConstrained("ab0", "aaa") shouldBe Some("aab")
      myPicker.lowerConstrained("aab", "aaa") shouldBe Some("aaabbbbb")
      myPicker.lowerConstrained("aaab", "aaa") shouldBe Some("aaaa")
    }
  }
}
