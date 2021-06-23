// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

private[appendonlydao] class TransactionsReaderSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks {
  "splitRange" should {
    "correctly split in equal ranges" in {
      TransactionsReader.splitRange(100L, 200L, 4, 10, 100) shouldBe Vector(
        EventsRange(100L, 125L),
        EventsRange(125L, 150L),
        EventsRange(150L, 175L),
        EventsRange(175L, 200L),
      )
    }

    "correctly split in non-equal ranges" in {
      TransactionsReader.splitRange(100L, 200L, 3, 10, 100) shouldBe Vector(
        EventsRange(100L, 134L),
        EventsRange(134L, 168L),
        EventsRange(168L, 200L),
      )
    }

    "output ranges of sizes at least minChunkSize" in {
      TransactionsReader.splitRange(100L, 200L, 3, 50, 100) shouldBe Vector(
        EventsRange(100L, 150L),
        EventsRange(150L, 200L),
      )
    }

    "output one range if minChunkSize higher than range size" in {
      TransactionsReader.splitRange(100L, 200L, 3, 200, 100) shouldBe Vector(
        EventsRange(100L, 200L)
      )
    }

    "use the effectiveChunkSize if maxChunkSize is higher than double of the minChunkSize" in {
      TransactionsReader.splitRange(100L, 200L, 3, 75, 90) shouldBe Vector(
        EventsRange(100L, 200L)
      )
    }

    "output only one range if minChunkSize greater than half the range" in {
      TransactionsReader.splitRange(100L, 200L, 3, 51, 100) shouldBe Vector(
        EventsRange(100L, 200L)
      )
    }

    "output only one range if minChunkSize is gteq to range size" in {
      TransactionsReader.splitRange(100L, 200L, 3, 100, 100) shouldBe Vector(
        EventsRange(100L, 200L)
      )
    }

    "output ranges of at most max chunk size" in {
      TransactionsReader.splitRange(100L, 200L, 1, 20, 50) shouldBe Vector(
        EventsRange(100L, 150L),
        EventsRange(150L, 200L),
      )
    }

    "throw if numberOfChunks below 1" in {
      intercept[IllegalArgumentException] {
        TransactionsReader.splitRange(100L, 200L, 0, 100, 100)
      }.getMessage shouldBe "You can only split a range in a strictly positive number of chunks (0)"
    }

    "satisfy the required properties" in {
      forAll(Gen.choose(1, 10), Gen.choose(1, 20), Gen.choose(1, 100)) {
        (minChunkSize, maxChunkSize, rangeSize) =>
          (minChunkSize >= 1 && minChunkSize <= 10) shouldBe true
          (maxChunkSize >= 1 && maxChunkSize <= 20) shouldBe true
          (rangeSize >= 1 && rangeSize <= 100) shouldBe true

          val ranges =
            TransactionsReader.splitRange(100L, rangeSize + 100L, 8, minChunkSize, maxChunkSize)

          val subRangeSizes = ranges.map { case EventsRange(startExclusive, endInclusive) =>
            endInclusive - startExclusive
          }
          subRangeSizes.sum shouldBe rangeSize

          val minEffectiveMaxChunkSize = minChunkSize * 2
          val minEffectiveMinChunkSize = math.ceil(maxChunkSize.toDouble / 2.toDouble).toInt

          subRangeSizes.forall(size =>
            size <= minEffectiveMaxChunkSize && size >= minEffectiveMinChunkSize
          ) shouldBe true
      }
    }

    "Gen.choose" in {
      val firstGen = Gen.choose(1, 10)
      val secondGen = Gen.choose(1, 20)
      val thirdGen = Gen.choose(1, 100)
      forAll(firstGen, secondGen, thirdGen) { (firstBound, secondBound, thirdBound) =>
        (firstBound >= 1 && firstBound <= 10) shouldBe true
        (secondBound >= 1 && secondBound <= 20) shouldBe true
        (thirdBound >= 1 && thirdBound <= 100) shouldBe true
        println("Checked")
      }
    }
  }
}
