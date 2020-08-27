// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, WordSpec}

class StructSpec extends WordSpec with Matchers with PropertyChecks {

  private[this] val List(f1, f2, f3) = List("f1", "f2", "f3").map(Ref.Name.assertFromString)

  "SortMap.fromSortedImmArray" should {

    "fail if the input list is not sorted" in {

      val negativeTestCases =
        Table(
          "list",
          ImmArray.empty,
          ImmArray(f1 -> 1),
          ImmArray(f1 -> 1, f1 -> 2),
          ImmArray(f1 -> 1, f2 -> 2, f3 -> 3),
        )

      val positiveTestCases = Table(
        "list",
        ImmArray(f1 -> 1, f2 -> 2, f3 -> 3, f1 -> 2),
        ImmArray(f2 -> 2, f3 -> 3, f1 -> 1),
      )

      forEvery(negativeTestCases)(l => Struct.fromSortedImmArray(l) shouldBe 'right)

      forEvery(positiveTestCases)(l => Struct.fromSortedImmArray(l) shouldBe 'left)

    }
  }

  "SortMap.apply" should {
    "sorted fields" in {

      val testCases =
        Table(
          "list",
          Struct(),
          Struct(f1 -> 1),
          Struct(f1 -> 1, f1 -> 2),
          Struct(f1 -> 1, f2 -> 2, f3 -> 3),
          Struct(f1 -> 1, f2 -> 2, f3 -> 3, f1 -> 2),
          Struct(f2 -> 2, f3 -> 3, f1 -> 1),
        )

      forEvery(testCases) { s =>
        s.names.toSeq shouldBe s.names.toSeq.sorted[String]
      }

    }
  }

}
