// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.backend.Conversions.IntArrayDBSerialization.{
  decodeFromByteArray,
  encodeToByteArray,
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

private[backend] trait StorageBackendTestsConversions
    extends Matchers
    with ScalaCheckDrivenPropertyChecks { this: AnyFlatSpec =>

  behavior of "StorageBackend (conversions)"

  it should "serialize and deserialize sets of ints to bytes correctly" in {
    import org.scalacheck.Gen

    val setGen: Gen[Set[Int]] =
      Gen.containerOf[Set, Int](Gen.choose(Int.MinValue, Int.MaxValue))

    forAll(setGen) { set =>
      val encoded = encodeToByteArray(set)

      decodeFromByteArray(encoded) should contain theSameElementsAs set
      if (set.isEmpty) {
        encoded shouldBe empty
      } else {
        encoded(0) shouldBe 1
        encoded.length shouldBe set.size * 4 + 1 // version byte + 4 bytes per int
      }
    }
  }
}
