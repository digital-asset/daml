// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  String255,
  String300,
}
import org.scalatest.wordspec.AnyWordSpec

final case class TestWrapper(override val str: String255) extends LengthLimitedStringWrapper
object TestWrapper extends LengthLimitedStringWrapperCompanion[String255, TestWrapper] {
  override def instanceName: String = "TestWrapper"
  override protected def companion: String255.type = String255
  override protected def factoryMethodWrapper(str: String255): TestWrapper = TestWrapper(str)
}

final case class TestWrapper2(override val str: String255) extends LengthLimitedStringWrapper
object TestWrapper2 extends LengthLimitedStringWrapperCompanion[String255, TestWrapper2] {
  override def instanceName: String = "TestWrapper2"
  override protected def companion: String255.type = String255
  override protected def factoryMethodWrapper(str: String255): TestWrapper2 = TestWrapper2(str)
}

class LengthLimitedStringWrapperTest extends AnyWordSpec with BaseTest {
  "TestWrapper" should {
    "have a correctly working .create" in {
      val ok = TestWrapper.create("123")
      val ok2 = TestWrapper.create("")
      val ok3 = TestWrapper.create("a" * 255)
      val not_ok = TestWrapper.create("a" * 256)

      ok.value.unwrap shouldBe "123"
      ok2.value.unwrap shouldBe ""
      ok3.value.unwrap shouldBe "a" * 255
      not_ok.left.value shouldBe a[String]
      not_ok.left.value should (include("maximum length of 255") and include("TestWrapper"))
    }

    "have a correctly working .tryCreate" in {
      val ok = TestWrapper.tryCreate("123")
      val ok2 = TestWrapper.tryCreate("")
      val ok3 = TestWrapper.tryCreate("a" * 255)

      ok.unwrap shouldBe "123"
      ok2.unwrap shouldBe ""
      ok3.unwrap shouldBe "a" * 255
      a[IllegalArgumentException] should be thrownBy TestWrapper.tryCreate("a" * 256)
    }

    "have equals and hashcode functions that work like we expect them to" in {
      val string = "123"
      val string_124 = "124"
      val limited = String255.tryCreate("123")
      val limited_300 = String300.tryCreate("123")
      val wrapper = TestWrapper.tryCreate("123")
      val wrapper2 = TestWrapper2.tryCreate("123")
      // comparisons between String and LengthLimitedString/LengthLimitedStringWrapper
      limited == string shouldBe true
      wrapper == string shouldBe true
      wrapper2 == string shouldBe true
      limited.hashCode() == string.hashCode() shouldBe true
      wrapper.hashCode() == string.hashCode() shouldBe true
      wrapper2.hashCode() == string.hashCode() shouldBe true

      limited == string_124 shouldBe false
      wrapper == string_124 shouldBe false
      wrapper2 == string_124 shouldBe false

      string == limited shouldBe false
      string == wrapper shouldBe false
      string == wrapper2 shouldBe false

      // sanity checks that we don't have otherwise unintended behaviour
      // comparisons between LengthLimitedString and LengthLimitedStringWrapper
      limited == wrapper shouldBe false
      wrapper == limited shouldBe false

      // comparisons between different LengthLimitedStringWrapper
      wrapper == wrapper2 shouldBe false
      limited == limited_300 shouldBe false
      val wrapper_same = TestWrapper.tryCreate("123")
      wrapper == wrapper_same shouldBe true

    }
  }
}
