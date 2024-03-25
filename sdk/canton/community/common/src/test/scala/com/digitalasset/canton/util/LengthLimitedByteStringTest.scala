// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class LengthLimitedByteStringTest extends AnyWordSpec with BaseTest {
  "LengthLimitedByteString256" should {
    "have a correctly working .create" in {
      val ok = ByteString256.create(ByteString.copyFrom("123".getBytes()))
      val ok2 = ByteString256.create(ByteString.EMPTY)
      val ok3 = ByteString256.create(ByteString.copyFrom(("a" * 256).getBytes()))
      val not_ok =
        ByteString256.create(ByteString.copyFrom(("a" * 257).getBytes()), Some("Incantation"))

      ok.value.unwrap shouldBe ByteString.copyFrom("123".getBytes())
      ok2.value.unwrap shouldBe ByteString.EMPTY
      ok3.value.unwrap shouldBe ByteString.copyFrom(("a" * 256).getBytes())
      not_ok.left.value shouldBe a[String]
      not_ok.left.value should (include("maximum length of 256") and include("Incantation"))
    }

    "have a correctly working .tryCreate" in {
      val ok = ByteString256.tryCreate(ByteString.copyFrom("123".getBytes()))
      val ok2 = ByteString256.tryCreate(ByteString.EMPTY)
      val ok3 = ByteString256.tryCreate(ByteString.copyFrom(("a" * 256).getBytes()))

      ok.unwrap shouldBe ByteString.copyFrom("123".getBytes())
      ok2.unwrap shouldBe ByteString.EMPTY
      ok3.unwrap shouldBe ByteString.copyFrom(("a" * 256).getBytes())
      a[IllegalArgumentException] should be thrownBy ByteString256
        .tryCreate(ByteString.copyFrom(("a" * 257).getBytes()))
    }

    "correctly create a empty bounded ByteString" in {
      val empty = ByteString256.empty
      empty shouldBe ByteString.EMPTY
    }

    "have equals and hashcode functions that work like we expect them to" in {
      val s = ByteString.copyFrom("s".getBytes())
      val bar = ByteString.copyFrom("bar".getBytes())

      val s256 = ByteString256.tryCreate(ByteString.copyFrom("s".getBytes()))

      s256.equals(s) shouldBe true
      s256.equals(s256) shouldBe true
      s256.equals(bar) shouldBe bar.equals(s256)

      s256.hashCode() == s.hashCode() shouldBe true
      s256.hashCode() == bar.hashCode() shouldBe false
    }
  }
}
