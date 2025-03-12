// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochLength
import org.scalatest.wordspec.AnyWordSpec

class IssConsensusModuleMetricsTest extends AnyWordSpec with BftSequencerBaseTest {

  "possibleVotesInEpoch" should {
    "work as expected" in {
      Table(
        ("votes", "epochLength", "segmentLeaders", "viewsCount", "expected"),
        (1, 1L, 1, 1L, 1),
        (1, 1L, 1, 2L, 2),
        (1, 2L, 1, 1L, 2),
        (2, 1L, 1, 1L, 2),
        (2, 1L, 1, 2L, 4),
        (2, 2L, 1, 2L, 6),
        (2, 2L, 2, 2L, 4),
        (2, 2L, 2, 3L, 6),
        (2, 2L, 2, 4L, 8),
      ).forEvery { (votes, epochLength, segmentLeaders, viewsCount, expected) =>
        val result = IssConsensusModuleMetrics.totalConsensusStageVotesInEpoch(
          votes,
          EpochLength(epochLength),
          segmentLeaders,
          viewsCount,
        )
        result shouldBe expected
      }
    }
  }
}
