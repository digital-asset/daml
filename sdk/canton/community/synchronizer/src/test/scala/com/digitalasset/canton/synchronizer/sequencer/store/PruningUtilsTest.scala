// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.PositiveFiniteDuration
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PruningUtilsTest extends AnyWordSpec with BaseTest with Matchers {

  private def ts(seconds: Long): CantonTimestamp =
    CantonTimestamp.Epoch.plusSeconds(seconds)

  "pruningTimeIntervals" should {
    "produce correct results" in {
      PruningUtils.pruningTimeIntervals(
        minO = None,
        upTo = CantonTimestamp.MaxValue,
        step = PositiveFiniteDuration.tryOfSeconds(10),
      ) shouldEqual Seq.empty

      PruningUtils.pruningTimeIntervals(
        minO = Some(ts(0)),
        upTo = ts(10),
        step = PositiveFiniteDuration.tryOfSeconds(10),
      ) shouldEqual Seq(
        CantonTimestamp.MinValue -> ts(0),
        ts(0) -> ts(10),
      )

      PruningUtils.pruningTimeIntervals(
        minO = Some(ts(0)),
        upTo = ts(10),
        step = PositiveFiniteDuration.tryOfSeconds(3),
      ) shouldEqual Seq(
        CantonTimestamp.MinValue -> ts(0),
        ts(0) -> ts(3),
        ts(3) -> ts(6),
        ts(6) -> ts(9),
        ts(9) -> ts(10),
      )
    }
  }
}
