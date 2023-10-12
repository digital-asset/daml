// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Id
import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class MapsUtilTest extends AnyWordSpec with BaseTest {
  "MapsUtil" should {

    "correctly group by multiple values" in {
      val m = Map[String, Int]("abc" -> 1, "cde" -> 2, "def" -> 3)
      MapsUtil.groupByMultipleM[Id, String, Char, Int](m)(s => s.toSet) shouldBe Map(
        'a' -> Set(1),
        'b' -> Set(1),
        'c' -> Set(1, 2),
        'd' -> Set(2, 3),
        'e' -> Set(2, 3),
        'f' -> Set(3),
      )
    }

  }
}
