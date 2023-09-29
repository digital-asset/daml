// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class TreeMapSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  "TreeMap.fromOrderedEntries should fails if the input list is not sorted" in {

    val negativeTestCases =
      Table(
        "list",
        List.empty,
        List("1" -> 1),
        List("1" -> 1, "2" -> 2, "3" -> 3),
      )

    val positiveTestCases = Table(
      "list",
      List("1" -> 1, "0" -> 2),
      List("1" -> 1, "2" -> 2, "3" -> 3, "1" -> 2),
      List("2" -> 2, "3" -> 3, "1" -> 1),
      List("1" -> 1, "1" -> 2),
      List("1" -> 1, "2" -> 2, "3" -> 3, "3" -> 2),
    )

    forAll(negativeTestCases) { l =>
      val treeMap = TreeMap.fromOrderedEntries(l)
      assert(treeMap.iterator sameElements l)
    }

    forAll(positiveTestCases)(l =>
      a[IllegalArgumentException] shouldBe thrownBy(TreeMap.fromOrderedEntries(l))
    )
  }

}
