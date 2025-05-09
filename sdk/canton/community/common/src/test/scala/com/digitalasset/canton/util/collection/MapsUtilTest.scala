// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.collection

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

    "build non conflicting maps" in {
      MapsUtil.toNonConflictingMap(Seq(1 -> 2, 2 -> 3)) shouldBe Right(Map(1 -> 2, 2 -> 3))
      MapsUtil.toNonConflictingMap(Seq(1 -> 2, 2 -> 3, 1 -> 2)) shouldBe Right(Map(1 -> 2, 2 -> 3))
      MapsUtil.toNonConflictingMap(Seq(1 -> 2, 1 -> 3)) shouldBe Left(Map(1 -> Set(2, 3)))
    }

    "compute intersection based on values" in {
      import MapsUtil.intersectValues

      val empty = Map.empty[String, Set[Int]]

      intersectValues(Map("1" -> Set(1)), empty) shouldBe Map.empty
      intersectValues(empty, Map("1" -> Set(1))) shouldBe Map.empty
      intersectValues(Map("1" -> Set(1)), Map("2" -> Set(2))) shouldBe Map.empty

      intersectValues(Map("1" -> Set(1)), Map("2" -> Set(2), "1" -> Set(1))) shouldBe Map(
        "1" -> Set(1)
      )

      intersectValues(
        Map("1" -> Set(1), "2" -> Set(20, 21, 22, 23)),
        Map("2" -> Set(20, 23, 25), "1" -> Set(1)),
      ) shouldBe Map(
        "1" -> Set(1),
        "2" -> Set(20, 23),
      )
    }

    "transpose" in {
      import MapsUtil.transpose

      transpose(Map(1 -> Set("a"), 2 -> Set("a"))) shouldBe Map("a" -> Set(1, 2))
      transpose(Map(1 -> Set("a", "b"))) shouldBe Map("a" -> Set(1), "b" -> Set(1))
      transpose(Map("a" -> Set.empty[Int], "b" -> Set(1))) shouldBe Map(1 -> Set("b"))
      transpose(Map.empty[Int, Set[String]]) shouldBe Map.empty[String, Set[Int]]
    }
  }
}
