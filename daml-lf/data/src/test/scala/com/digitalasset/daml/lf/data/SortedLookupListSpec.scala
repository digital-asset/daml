// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalatest.prop.{PropertyChecks}
import org.scalatest.{Matchers, WordSpec}

class SortedLookupListSpec extends WordSpec with Matchers with PropertyChecks {

  "SortMap.fromImmArray should fails if the input list contians duplicate keys" in {

    val negativeTestCases = Table(
      "list",
      ImmArray.empty[(Utf8String, Int)],
      ImmArray(Utf8String("1") -> 1),
      ImmArray(Utf8String("1") -> 1, Utf8String("2") -> 2, Utf8String("3") -> 3),
      ImmArray(Utf8String("2") -> 2, Utf8String("3") -> 3, Utf8String("1") -> 1)
    )

    val positiveTestCases =
      Table(
        "list",
        ImmArray(Utf8String("1") -> 1, Utf8String("1") -> 2),
        ImmArray(
          Utf8String("1") -> 1,
          Utf8String("2") -> 2,
          Utf8String("3") -> 3,
          Utf8String("1") -> 2))

    forAll(negativeTestCases)(l => SortedLookupList.fromImmArray(l) shouldBe 'right)

    forAll(positiveTestCases)(l => SortedLookupList.fromImmArray(l) shouldBe 'left)

  }

  "SortMap.fromSortedImmArray should fails if the input list is not sorted" in {

    val negativeTestCases =
      Table(
        "list",
        ImmArray.empty[(Utf8String, Int)],
        ImmArray(Utf8String("1") -> 1),
        ImmArray(Utf8String("1") -> 1, Utf8String("2") -> 2, Utf8String("3") -> 3))

    val positiveTestCases = Table(
      "list",
      ImmArray(Utf8String("1") -> 1, Utf8String("1") -> 2),
      ImmArray(
        Utf8String("1") -> 1,
        Utf8String("2") -> 2,
        Utf8String("3") -> 3,
        Utf8String("1") -> 2),
      ImmArray(Utf8String("2") -> 2, Utf8String("3") -> 3, Utf8String("1") -> 1)
    )

    forAll(negativeTestCases)(l => SortedLookupList.fromSortedImmArray(l) shouldBe 'right)

    forAll(positiveTestCases)(l => SortedLookupList.fromSortedImmArray(l) shouldBe 'left)

  }

}
