// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.{BaseTest, RequestCounter, SequencerCounter}
import org.scalatest.funspec.PathAnyFunSpec

trait RequestCounterAllocatorTest extends PathAnyFunSpec with BaseTest {

  def requestCounterAllocator(
      mk: (RequestCounter, SequencerCounter) => RequestCounterAllocator
  ): Unit = {

    describe("when created starting from 0") {
      val rca = mk(RequestCounter(0), SequencerCounter(0))

      describe("skip sequencer counters below the clean replay prehead") {
        forEvery(Seq(SequencerCounter(Long.MinValue), SequencerCounter(-1))) { sc =>
          rca.allocateFor(sc) shouldBe None
        }
      }

      describe("allocation should start at 0") {
        rca.allocateFor(SequencerCounter(0)) shouldBe Some(SequencerCounter(0))
      }

      describe("allocation should be consecutive") {
        val scs =
          Table(
            ("sequencer counter", "expected request counter"),
            (SequencerCounter(0), RequestCounter(0)),
            (SequencerCounter(1), RequestCounter(1)),
            (SequencerCounter(4), RequestCounter(2)),
            (SequencerCounter(8), RequestCounter(3)),
            (SequencerCounter(10), RequestCounter(4)),
          )

        forEvery(scs) { (sc, expRc) =>
          rca.allocateFor(sc) shouldBe Some(expRc)
        }

        describe("allocation is idempotent for consecutive calls") {
          rca.allocateFor(SequencerCounter(10)) shouldBe Some(SequencerCounter(4))
        }

        describe(
          "allocation should fail if a sequence counter comes twice with an intermediate call"
        ) {
          loggerFactory.assertInternalError[IllegalStateException](
            rca.allocateFor(SequencerCounter(8)),
            _.getMessage shouldBe "Cannot allocate request counter for confirmation request with counter 8 because a lower request counter has already been allocated to 10",
          )
        }

        describe("allocation should fail if a sequence counter comes too late") {
          loggerFactory.assertInternalError[IllegalStateException](
            rca.allocateFor(SequencerCounter(9)),
            _.getMessage shouldBe "Cannot allocate request counter for confirmation request with counter 9 because a lower request counter has already been allocated to 10",
          )
        }
      }

      describe(s"allocation should succeed for ${Long.MaxValue - 1}") {
        rca.allocateFor(SequencerCounter.MaxValue - 1) shouldBe Some(SequencerCounter(0))

        it("and then fail because of an invalid sequence counter") {
          loggerFactory.assertInternalError[IllegalArgumentException](
            rca.allocateFor(SequencerCounter.MaxValue),
            _.getMessage shouldBe "Sequencer counter 9223372036854775807 cannot be used.",
          )
        }
      }
    }

    describe(s"when created with ${RequestCounter.MaxValue - 1}") {
      val rca = mk(RequestCounter.MaxValue - 1, SequencerCounter(0))
      it("should allocate only one request counter and then fail") {
        rca.allocateFor(SequencerCounter(0)) shouldBe Some(RequestCounter.MaxValue - 1)
        loggerFactory.assertInternalError[IllegalStateException](
          rca.allocateFor(SequencerCounter(1)),
          _.getMessage shouldBe "No more request counters can be allocated because the request counters have reached 9223372036854775806.",
        )
      }
    }

    describe(s"when created with non-zero clean replay sequencer counter") {
      val cleanReplaySc = SequencerCounter(100)
      val rca = mk(RequestCounter(0), cleanReplaySc)

      it("skip allocations below") {
        forEvery(Seq(SequencerCounter(Long.MinValue), SequencerCounter(0), SequencerCounter(99))) {
          sc => rca.allocateFor(sc) shouldBe None
        }
      }
    }

    describe("when skiping repair requests") {
      val cleanReplaySc = SequencerCounter(100)
      val rca = mk(RequestCounter(2), cleanReplaySc)

      it("interleave allocation and skipping") {
        rca.allocateFor(SequencerCounter(100)) shouldBe Some(RequestCounter(2))
        rca.skipRequestCounter(RequestCounter(3))
        rca.skipRequestCounter(RequestCounter(4))
        rca.allocateFor(SequencerCounter(101)) shouldBe Some(RequestCounter(5))
        rca.skipRequestCounter(RequestCounter(6))
        rca.allocateFor(SequencerCounter(102)) shouldBe Some(RequestCounter(7))
      }

      it("complain about gaps") {
        loggerFactory.assertInternalError[IllegalArgumentException](
          rca.skipRequestCounter(RequestCounter(3)),
          _.getMessage shouldBe "Cannot skip request counter 3 other than the next request counter 2",
        )
        loggerFactory.assertInternalError[IllegalArgumentException](
          rca.skipRequestCounter(RequestCounter(1)),
          _.getMessage shouldBe "Cannot skip request counter 1 other than the next request counter 2",
        )
        rca.allocateFor(SequencerCounter(102)) shouldBe Some(RequestCounter(2))
      }
    }
  }
}

class RequestCounterAllocatorImplTest extends RequestCounterAllocatorTest {

  describe("RequestCounterAllocatorImplTest") {
    behave like requestCounterAllocator { (initRc, cleanReplaySc) =>
      new RequestCounterAllocatorImpl(initRc, cleanReplaySc, loggerFactory)
    }

    assertThrows[IllegalArgumentException](
      new RequestCounterAllocatorImpl(RequestCounter.MaxValue, SequencerCounter(0), loggerFactory)
    )
  }
}
