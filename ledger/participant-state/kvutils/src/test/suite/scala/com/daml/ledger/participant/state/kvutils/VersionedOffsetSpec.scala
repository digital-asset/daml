// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class VersionedOffsetSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import OffsetGen._

  "VersionedOffset" when {
    "constructing" should {
      "construct and split up an offset" in {
        forAll(genVersion, genHighest, genMiddle, genLowest) { (version, highest, middle, lowest) =>
          val offset = VersionedOffset.of(version, highest, middle, lowest)

          offset.version should be(version)
          offset.highest should be(highest)
          offset.middle should be(middle)
          offset.lowest should be(lowest)
        }
      }

      "fail on a highest that is out of range" in {
        forAll(genVersion, genOutOfRangeHighest, genMiddle, genLowest) {
          (version, highest, middle, lowest) =>
            val builder = new VersionedOffsetBuilder(version)
            (the[IllegalArgumentException] thrownBy builder.of(highest, middle, lowest)
              should have message s"requirement failed: highest ($highest) is out of range [0, ${VersionedOffset.MaxHighest}]")
        }
      }

      "fail on a negative middle index" in {
        forAll(genVersion, genHighest, genOutOfRangeMiddle, genLowest) {
          (version, highest, middle, lowest) =>
            val builder = new VersionedOffsetBuilder(version)
            (the[IllegalArgumentException] thrownBy builder.of(highest, middle, lowest)
              should have message s"requirement failed: middle ($middle) is lower than 0")
        }
      }

      "fail on a negative lowest index" in {
        forAll(genVersion, genHighest, genMiddle, genOutOfRangeLowest) {
          (version, highest, middle, lowest) =>
            val builder = new VersionedOffsetBuilder(version)
            (the[IllegalArgumentException] thrownBy builder.of(highest, middle, lowest)
              should have message s"requirement failed: lowest ($lowest) is lower than 0")
        }
      }
    }

    "mutating" should {
      "only change individual indexes" in {
        forAll(genVersion, genHighest, genMiddle, genLowest, genLowest) {
          (version, highest, middle, lowest, newLowest) =>
            val offset = VersionedOffset.of(version, highest, middle, lowest)

            val modifiedOffset = offset.setLowest(newLowest)

            modifiedOffset.version should be(version)
            modifiedOffset.highest should be(highest)
            modifiedOffset.middle should be(middle)
            modifiedOffset.lowest should be(newLowest)
        }
      }
    }
  }
}
