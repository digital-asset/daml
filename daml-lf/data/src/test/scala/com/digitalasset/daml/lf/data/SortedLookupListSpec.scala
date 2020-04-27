// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalatest.prop.{PropertyChecks}
import org.scalatest.{Matchers, WordSpec}

class SortedLookupListSpec extends WordSpec with Matchers with PropertyChecks {

  "SortMap.fromImmArray should fails if the input list contians duplicate keys" in {

    val negativeTestCases = Table(
      "list",
      ImmArray.empty[(String, Int)],
      ImmArray("1" -> 1),
      ImmArray("1" -> 1, "2" -> 2, "3" -> 3),
      ImmArray("2" -> 2, "3" -> 3, "1" -> 1)
    )

    val positiveTestCases =
      Table("list", ImmArray("1" -> 1, "1" -> 2), ImmArray("1" -> 1, "2" -> 2, "3" -> 3, "1" -> 2))

    forAll(negativeTestCases)(l => SortedLookupList.fromImmArray(l) shouldBe 'right)

    forAll(positiveTestCases)(l => SortedLookupList.fromImmArray(l) shouldBe 'left)

  }

  "SortMap.fromSortedImmArray should fails if the input list is not sorted" in {

    val negativeTestCases =
      Table(
        "list",
        ImmArray.empty[(String, Int)],
        ImmArray("1" -> 1),
        ImmArray("1" -> 1, "2" -> 2, "3" -> 3))

    val positiveTestCases = Table(
      "list",
      ImmArray("1" -> 1, "1" -> 2),
      ImmArray("1" -> 1, "2" -> 2, "3" -> 3, "1" -> 2),
      ImmArray("2" -> 2, "3" -> 3, "1" -> 1)
    )

    forAll(negativeTestCases)(l => SortedLookupList.fromSortedImmArray(l) shouldBe 'right)

    forAll(positiveTestCases)(l => SortedLookupList.fromSortedImmArray(l) shouldBe 'left)

  }

}
