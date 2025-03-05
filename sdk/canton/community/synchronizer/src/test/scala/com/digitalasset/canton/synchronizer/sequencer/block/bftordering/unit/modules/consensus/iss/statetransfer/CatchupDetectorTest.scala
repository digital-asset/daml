// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.DefaultCatchupDetector
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import org.scalatest.wordspec.AnyWordSpec

import CatchupDetectorTest.{membership, mySequencerId, otherSequencerId}

class CatchupDetectorTest extends AnyWordSpec with BftSequencerBaseTest {

  "track the latest epoch for active peers and determine if the node needs to switch to catch-up mode" in {
    val catchupDetector = new DefaultCatchupDetector(membership, loggerFactory)

    catchupDetector.updateLatestKnownPeerEpoch(otherSequencerId, EpochNumber.First) shouldBe true
    catchupDetector.updateLatestKnownPeerEpoch(otherSequencerId, EpochNumber.First) shouldBe false

    catchupDetector.shouldCatchUp(localEpoch = EpochNumber.First) shouldBe false

    catchupDetector.updateLatestKnownPeerEpoch(otherSequencerId, EpochNumber(1)) shouldBe true
    catchupDetector.updateLatestKnownPeerEpoch(otherSequencerId, EpochNumber.First) shouldBe false

    catchupDetector.shouldCatchUp(localEpoch = EpochNumber.First) shouldBe false

    catchupDetector.updateLatestKnownPeerEpoch(otherSequencerId, EpochNumber(2)) shouldBe true

    catchupDetector.shouldCatchUp(localEpoch = EpochNumber.First) shouldBe true

    val newMembership = Membership.forTesting(mySequencerId, otherPeers = Set.empty)
    catchupDetector.updateMembership(newMembership)

    catchupDetector.updateLatestKnownPeerEpoch(otherSequencerId, EpochNumber(2)) shouldBe false

    catchupDetector.shouldCatchUp(localEpoch = EpochNumber.First) shouldBe false
  }
}

object CatchupDetectorTest {

  private val mySequencerId = fakeSequencerId("self")
  private val otherSequencerId = fakeSequencerId("other")
  private val membership = Membership.forTesting(mySequencerId, Set(otherSequencerId))
}
