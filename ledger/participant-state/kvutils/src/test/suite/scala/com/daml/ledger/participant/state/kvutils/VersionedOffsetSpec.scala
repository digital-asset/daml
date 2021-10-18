// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class VersionedOffsetSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import OffsetGen._

  "VersionedOffset" should {
    "split up an offset" in {
      forAll(genVersion, genHighest, genMiddle, genLowest) { (version, highest, middle, lowest) =>
        val builder = new VersionedOffsetBuilder(version)
        val offset = builder.of(highest, middle, lowest)

        val splitOffset = VersionedOffset(offset)
        splitOffset.version should be(version)
        splitOffset.highest should be(highest)
        splitOffset.middle should be(middle)
        splitOffset.lowest should be(lowest)
      }
    }

    "only change individual indexes" in {
      forAll(genVersion, genHighest, genMiddle, genLowest, genLowest) {
        (version, highest, middle, lowest, newLowest) =>
          val builder = new VersionedOffsetBuilder(version)
          val offset = VersionedOffset(builder.of(highest, middle, lowest))

          val modifiedOffset = offset.setLowest(newLowest)

          modifiedOffset.offset should be(builder.of(highest, middle, newLowest))
          modifiedOffset.version should be(version)
          modifiedOffset.highest should be(highest)
          modifiedOffset.middle should be(middle)
          modifiedOffset.lowest should be(newLowest)
      }
    }
  }
}
