// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, WordSpec}

class SortedMapSpec extends WordSpec with Matchers with PropertyChecks {

  "SortMap.fromList should fails if the input list contians duplicate keys" in {

    val negativeTestCases = Table(
      "list",
      List.empty[(String, Int)],
      List("1" -> 1),
      List("1" -> 1, "2" -> 2, "3" -> 3),
      List("2" -> 2, "3" -> 3, "1" -> 1))

    val positiveTestCases =
      Table("list", List("1" -> 1, "1" -> 2), List("1" -> 1, "2" -> 2, "3" -> 3, "1" -> 2))

    forAll(negativeTestCases)(l => SortedMap.fromList(l) shouldBe 'right)

    forAll(positiveTestCases)(l => SortedMap.fromList(l) shouldBe 'left)

  }

  "SortMap.fromSortedList should fails if the input list is not sorted" in {

    val negativeTestCases =
      Table("list", List.empty[(String, Int)], List("1" -> 1), List("1" -> 1, "2" -> 2, "3" -> 3))

    val positiveTestCases = Table(
      "list",
      List("1" -> 1, "1" -> 2),
      List("1" -> 1, "2" -> 2, "3" -> 3, "1" -> 2),
      List("2" -> 2, "3" -> 3, "1" -> 1))

    forAll(negativeTestCases)(l => SortedMap.fromSortedList(l) shouldBe 'right)

    forAll(positiveTestCases)(l => SortedMap.fromSortedList(l) shouldBe 'left)

  }

}
