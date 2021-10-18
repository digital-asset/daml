// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class VersionedOffsetMutatorSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  import OffsetGen._

  "VersionedOffsetMutator" should {
    "only change individual indexes" in {
      forAll(genVersion, genHighest, genMiddle, genLowest, genLowest) {
        (version, highest, middle, lowest, newLowest) =>
          val builder = new VersionedOffsetBuilder(version)
          val offset = builder.of(highest, middle, lowest)

          val modifiedOffset = VersionedOffsetMutator.setLowest(offset, newLowest)

          val components = VersionedOffset(modifiedOffset)
          components.version should be(version)
          components.highest should be(highest)
          components.middle should be(middle)
          components.lowest should be(newLowest)
      }
    }
  }
}
