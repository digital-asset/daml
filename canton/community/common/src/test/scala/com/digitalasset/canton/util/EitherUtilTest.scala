// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.EitherUtil.{RichEither, RichEitherIterable}
import org.scalatest.wordspec.AnyWordSpec

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class EitherUtilTest extends AnyWordSpec with BaseTest {
  "EitherUtil" should {
    "construct an Either[L, Unit]" in {
      EitherUtil.condUnitE(true, "sadness") shouldBe Right(())
      EitherUtil.condUnitE(false, "sadness") shouldBe Left("sadness")
    }
  }

  "EitherUtil.RichEither" should {
    val left: Either[String, Int] = Left("sadness")
    val right: Either[String, Int] = Right(42)

    "implement tap left" in {
      var counter = 0

      right.tapLeft(_ => counter += 1) shouldBe right
      counter shouldBe 0

      left.tapLeft(_ => counter += 1) shouldBe left
      counter shouldBe 1
    }

    "implement tap right" in {
      var counter = 0

      left.tapRight(_ => counter += 1) shouldBe left
      counter shouldBe 0

      right.tapRight(_ => counter += 1) shouldBe right
      counter shouldBe 1
    }
  }

  "EitherUtil.RichEitherIterable" should {
    val eithers = List(Right(3), Left("a"), Right(2), Right(1), Left("b"))

    "implement collect left" in {
      Nil.collectLeft shouldBe Nil
      eithers.collectLeft shouldBe List("a", "b")
    }

    "implement collect right" in {
      Nil.collectRight shouldBe Nil
      eithers.collectRight shouldBe List(3, 2, 1)
    }
  }
}
