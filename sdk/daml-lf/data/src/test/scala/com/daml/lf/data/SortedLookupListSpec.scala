// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SortedLookupListSpec extends AnyWordSpec with Matchers with ScalaCheckPropertyChecks {

  "SortMap.fromImmArray should fails if the input list contians duplicate keys" in {

    val negativeTestCases = Table(
      "list",
      ImmArray.Empty,
      ImmArray("1" -> 1),
      ImmArray("1" -> 1, "2" -> 2, "3" -> 3),
      ImmArray("2" -> 2, "3" -> 3, "1" -> 1),
    )

    val positiveTestCases =
      Table("list", ImmArray("1" -> 1, "1" -> 2), ImmArray("1" -> 1, "2" -> 2, "3" -> 3, "1" -> 2))

    forAll(negativeTestCases)(l => SortedLookupList.fromImmArray(l) shouldBe a[Right[_, _]])

    forAll(positiveTestCases)(l => SortedLookupList.fromImmArray(l) shouldBe a[Left[_, _]])

  }

  "SortMap.fromSortedImmArray should fails if the input list is not sorted" in {

    val negativeTestCases =
      Table(
        "list",
        ImmArray.empty[(String, Int)],
        ImmArray("1" -> 1),
        ImmArray("1" -> 1, "2" -> 2, "3" -> 3),
      )

    val positiveTestCases = Table(
      "list",
      ImmArray("1" -> 1, "1" -> 2),
      ImmArray("1" -> 1, "2" -> 2, "3" -> 3, "1" -> 2),
      ImmArray("2" -> 2, "3" -> 3, "1" -> 1),
    )

    forAll(negativeTestCases)(l => SortedLookupList.fromOrderedImmArray(l) shouldBe a[Right[_, _]])

    forAll(positiveTestCases)(l => SortedLookupList.fromOrderedImmArray(l) shouldBe a[Left[_, _]])

  }

}
