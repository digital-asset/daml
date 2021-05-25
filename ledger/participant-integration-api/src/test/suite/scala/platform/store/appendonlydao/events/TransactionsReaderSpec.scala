// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

private[appendonlydao] class TransactionsReaderSpec extends AnyWordSpec with Matchers {
  "splitRange" should {
    "correctly split in equal ranges" in {
      TransactionsReader.splitRange(100L, 200L, 4, 10) shouldBe Seq(
        EventsRange(100L, 125L),
        EventsRange(125L, 150L),
        EventsRange(150L, 175L),
        EventsRange(175L, 200L),
      )
    }

    "correctly split in non-equal ranges" in {
      TransactionsReader.splitRange(100L, 200L, 3, 10) shouldBe Seq(
        EventsRange(100L, 134L),
        EventsRange(134L, 168L),
        EventsRange(168L, 200L),
      )
    }

    "output only one range if target range below minChunkSize" in {
      TransactionsReader.splitRange(100L, 200L, 3, 100) shouldBe Seq(
        EventsRange(100L, 200L)
      )
    }

    "output only one range if numberOfChunks is 1" in {
      TransactionsReader.splitRange(100L, 200L, 3, 100) shouldBe Seq(
        EventsRange(100L, 200L)
      )
    }

    "throw if numberOfChunks below 1" in {
      intercept[IllegalArgumentException] {
        TransactionsReader.splitRange(100L, 200L, 0, 100)
      }.getMessage shouldBe "You can only split a range in a strictly positive number of chunks (0)"
    }
  }
}
