// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.{
  CatchupDetector,
  StateTransferDetector,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer.CatchupDetectorTest.{
  membership,
  mySequencerId,
  otherSequencerId,
}
import org.scalatest.wordspec.AnyWordSpec

class CatchupDetectorTest extends AnyWordSpec with BftSequencerBaseTest {

  "track the latest epoch for active peers and determine if the node needs to switch to catch-up mode" in {
    var isInStateTransfer = false
    val catchupDetector =
      new CatchupDetector(
        membership,
        new StateTransferDetector {
          override def inStateTransfer: Boolean = isInStateTransfer
        },
      )

    catchupDetector.updateLatestKnownPeerEpoch(otherSequencerId, EpochNumber.First) shouldBe true
    catchupDetector.updateLatestKnownPeerEpoch(otherSequencerId, EpochNumber.First) shouldBe false

    catchupDetector.shouldCatchUp(localEpoch = EpochNumber.First) shouldBe false

    catchupDetector.updateLatestKnownPeerEpoch(otherSequencerId, EpochNumber(1)) shouldBe true
    catchupDetector.updateLatestKnownPeerEpoch(otherSequencerId, EpochNumber.First) shouldBe false

    catchupDetector.shouldCatchUp(localEpoch = EpochNumber.First) shouldBe false

    catchupDetector.updateLatestKnownPeerEpoch(otherSequencerId, EpochNumber(2)) shouldBe true

    catchupDetector.shouldCatchUp(localEpoch = EpochNumber.First) shouldBe true
    isInStateTransfer = true
    catchupDetector.shouldCatchUp(localEpoch = EpochNumber.First) shouldBe false
    isInStateTransfer = false
    catchupDetector.shouldCatchUp(localEpoch = EpochNumber.First) shouldBe true

    val newMembership = Membership(mySequencerId, otherPeers = Set.empty)
    catchupDetector.updateMembership(newMembership)

    catchupDetector.updateLatestKnownPeerEpoch(otherSequencerId, EpochNumber(2)) shouldBe false

    catchupDetector.shouldCatchUp(localEpoch = EpochNumber.First) shouldBe false
  }
}

object CatchupDetectorTest {

  private val mySequencerId = fakeSequencerId("self")
  private val otherSequencerId = fakeSequencerId("other")
  private val membership = Membership(mySequencerId, Set(otherSequencerId))
}
