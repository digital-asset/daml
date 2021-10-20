// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class KVOffsetBuilderSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import OffsetGen._

  "VersionedOffsetBuilder" should {
    "construct and extract" in {
      forAll(genVersion, genHighest, genMiddle, genLowest) { (version, highest, middle, lowest) =>
        val builder = new KVOffsetBuilder(version)
        val offset = builder.of(highest, middle, lowest)

        val splitOffset = KVOffset(offset)
        splitOffset.version should be(version)
        splitOffset.highest should be(highest)
        splitOffset.middle should be(middle)
        splitOffset.lowest should be(lowest)
      }
    }

    "extract the highest index" in {
      forAll(genVersion, genHighest, genMiddle, genLowest) { (version, highest, middle, lowest) =>
        val builder = new KVOffsetBuilder(version)
        val offset = builder.of(highest, middle, lowest)

        builder.highestIndex(offset) should be(highest)
      }
    }

    "fail on a wrong version" in {
      forAll(genDifferentVersions, genHighest, genMiddle, genLowest) {
        case ((versionA, versionB), highest, middle, lowest) =>
          val offset = new KVOffsetBuilder(versionA).of(highest, middle, lowest)
          val offsetBuilder = new KVOffsetBuilder(versionB)
          val testedMethods =
            List(offsetBuilder.version(_), offsetBuilder.highestIndex(_), offsetBuilder.split(_))

          testedMethods.foreach { method =>
            (the[IllegalArgumentException] thrownBy method(offset)
              should have message s"requirement failed: wrong version $versionA, should be $versionB")
          }
      }
    }

    "test the version of the offset, returning `true` on a match" in {
      forAll(genVersion, genHighest, genMiddle, genLowest) { (version, highest, middle, lowest) =>
        val offsetBuilder = new KVOffsetBuilder(version)
        val offset = offsetBuilder.of(highest, middle, lowest)

        offsetBuilder.matchesVersionOf(offset) should be(true)
      }
    }

    "test the version of the offset, returning `false` on a mismatch" in {
      forAll(genDifferentVersions, genHighest, genMiddle, genLowest) {
        case ((versionA, versionB), highest, middle, lowest) =>
          val offset = new KVOffsetBuilder(versionA).of(highest, middle, lowest)
          val offsetBuilder = new KVOffsetBuilder(versionB)

          offsetBuilder.matchesVersionOf(offset) should be(false)
      }
    }
  }
}
