// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.DefaultCatchupDetector
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import org.scalatest.wordspec.AnyWordSpec

import CatchupDetectorTest.{membership, myId, otherId}

class CatchupDetectorTest extends AnyWordSpec with BftSequencerBaseTest {

  "track the latest epoch for active nodes and determine if the node needs to switch to catch-up mode" in {
    val catchupDetector = new DefaultCatchupDetector(membership, loggerFactory)

    catchupDetector.updateLatestKnownNodeEpoch(otherId, EpochNumber.First) shouldBe true
    catchupDetector.updateLatestKnownNodeEpoch(otherId, EpochNumber.First) shouldBe false

    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe None

    catchupDetector.updateLatestKnownNodeEpoch(otherId, EpochNumber(1)) shouldBe true
    catchupDetector.updateLatestKnownNodeEpoch(otherId, EpochNumber.First) shouldBe false

    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe None

    val latestKnownNodeEpoch = EpochNumber(2)
    catchupDetector.updateLatestKnownNodeEpoch(otherId, latestKnownNodeEpoch) shouldBe true

    // -1 because the latest known epoch may be in progress
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe Some(
      EpochNumber(latestKnownNodeEpoch - 1)
    )

    val newMembership = Membership.forTesting(myId, otherNodes = Set.empty)
    catchupDetector.updateMembership(newMembership)

    catchupDetector.updateLatestKnownNodeEpoch(otherId, latestKnownNodeEpoch) shouldBe false

    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe None
  }
}

object CatchupDetectorTest {

  private val myId = BftNodeId("self")
  private val otherId = BftNodeId("other")
  private val membership = Membership.forTesting(myId, Set(otherId))
}
