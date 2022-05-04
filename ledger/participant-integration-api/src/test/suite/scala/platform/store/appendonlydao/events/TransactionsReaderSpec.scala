// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import org.scalacheck.{Gen, Shrink}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

private[dao] class TransactionsReaderSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks {
  "splitRange" should {
    "correctly split in equal ranges by number of chunks" in {
      TransactionsReader.splitRange(
        startExclusive = 100L,
        endInclusive = 200L,
        numberOfChunks = 4,
        maxChunkSize = 100,
      ) shouldBe Vector(
        EventsRange(100L, 125L),
        EventsRange(125L, 150L),
        EventsRange(150L, 175L),
        EventsRange(175L, 200L),
      )
    }

    "fairly split in non-equal ranges by number of chunks" in {
      TransactionsReader.splitRange(
        startExclusive = 100L,
        endInclusive = 200L,
        numberOfChunks = 6,
        maxChunkSize = 100,
      ) shouldBe Vector(
        EventsRange(100L, 117L),
        EventsRange(117L, 134L),
        EventsRange(134L, 151L),
        EventsRange(151L, 168L),
        EventsRange(168L, 184L),
        EventsRange(184L, 200L),
      )
    }

    "output ranges of at most max chunk size" in {
      TransactionsReader.splitRange(
        startExclusive = 100L,
        endInclusive = 200L,
        numberOfChunks = 1,
        maxChunkSize = 50,
      ) shouldBe Vector(
        EventsRange(100L, 150L),
        EventsRange(150L, 200L),
      )
    }

    "output one range if maxChunkSize/10 (as minimum chunk size) is higher than range size" in {
      TransactionsReader.splitRange(
        startExclusive = 100L,
        endInclusive = 110L,
        numberOfChunks = 3,
        maxChunkSize = 100,
      ) shouldBe Vector(
        EventsRange(100L, 110L)
      )
    }

    "prioritize satisfying the maxChunkSize over numberOfChunks" in {
      TransactionsReader
        .splitRange(
          startExclusive = 0L,
          endInclusive = 3L,
          numberOfChunks = 2,
          maxChunkSize = 1,
        ) shouldBe Vector(
        EventsRange(0L, 1L),
        EventsRange(1L, 2L),
        EventsRange(2L, 3L),
      )
    }

    "prioritize satisfying a minimum chunk size of maxChunkSize/10 over numberOfChunks" in {
      TransactionsReader
        .splitRange(
          startExclusive = 0L,
          endInclusive = 6L,
          numberOfChunks = 6,
          maxChunkSize = 20,
        ) shouldBe Vector(
        EventsRange(0L, 2L),
        EventsRange(2L, 4L),
        EventsRange(4L, 6L),
      )
    }

    "work for max chunk size of 1" in {
      TransactionsReader
        .splitRange(
          startExclusive = 5L,
          endInclusive = 7L,
          numberOfChunks = 10,
          maxChunkSize = 1,
        ) shouldBe Vector(
        EventsRange(5L, 6L),
        EventsRange(6L, 7L),
      )
    }

    "return empty vector if startExclusive and endInclusive are equal" in {
      TransactionsReader
        .splitRange(
          startExclusive = 11L,
          endInclusive = 11L,
          numberOfChunks = 100,
          maxChunkSize = 100,
        ) shouldBe Vector.empty[EventsRange[Long]]
    }

    "throw if numberOfChunks is below 1" in {
      intercept[IllegalArgumentException] {
        TransactionsReader.splitRange(
          startExclusive = 100L,
          endInclusive = 200L,
          numberOfChunks = 0,
          maxChunkSize = 100,
        )
      }.getMessage shouldBe "requirement failed: You can only split a range in a strictly positive number of chunks (0)"
    }

    "throw if maxChunkSize is below 1" in {
      intercept[IllegalArgumentException] {
        TransactionsReader.splitRange(
          startExclusive = 100L,
          endInclusive = 200L,
          numberOfChunks = 100,
          maxChunkSize = 0,
        )
      }.getMessage shouldBe "requirement failed: Maximum chunk size must be strictly positive, but was 0"
    }

    "startExclusive is after endInclusive" in {
      intercept[IllegalArgumentException] {
        TransactionsReader.splitRange(
          startExclusive = 300L,
          endInclusive = 200L,
          numberOfChunks = 100,
          maxChunkSize = 10,
        )
      }.getMessage shouldBe "requirement failed: Range size should be positive but got bounds (300, 200]"
    }

    "satisfy the required properties" in {
      // Disable shrinking in forAll in order to ensure that the generator bounds are respected
      // in case of assertion failures
      implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny

      val maxChunkSize_Gen = Gen.choose(1, 20)
      val pageSize_Gen = Gen.choose(1L, 100L)
      val numberOfChunks_Gen = Gen.choose(1, 10)

      forAll(
        maxChunkSize_Gen,
        pageSize_Gen,
        numberOfChunks_Gen,
        minSuccessful(1000),
      ) { (maxChunkSize, rangeSize, numberOfChunks) =>
        val ranges =
          TransactionsReader.splitRange(
            startExclusive = 0L,
            endInclusive = rangeSize,
            numberOfChunks = numberOfChunks,
            maxChunkSize = maxChunkSize,
          )

        /* Assert non-empty ranges */
        ranges.size should be > 0

        /* Assert ranges are gap-less and non-overlapping */
        ranges.foldLeft(0L) {
          case (lastStartExclusive, EventsRange(startExclusive, endInclusive)) =>
            startExclusive shouldBe lastStartExclusive
            endInclusive
        }

        ranges.last.endInclusive shouldBe rangeSize

        val subRangeSizes = ranges.map { case EventsRange(startExclusive, endInclusive) =>
          endInclusive - startExclusive
        }

        /* Assert page sizes satisfy bounds and are similar in length */
        val maxSubrangeSize = subRangeSizes.max

        subRangeSizes.foreach { size =>
          size should be <= maxChunkSize.toLong
          size should be >= maxSubrangeSize - 1L
          size should be >= Math.min(maxChunkSize.toLong / 10, rangeSize)
          size should be > 0L
        }
      }
    }
  }
}
